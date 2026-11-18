package engine

import (
	"lamina/internal/catalog"
	"lamina/internal/datum"
	"lamina/internal/query"
)

func (d *DB) Query(q query.Request) (query.Result, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return query.Result{}, err
	}
	return d.queryLocked(q)
}

func (d *DB) queryLocked(q query.Request) (query.Result, error) {
	if q.Temporary {
		if q.Join != nil {
			return d.queryJoinLocked(q)
		}
		return d.queryTemporaryLocked(q)
	}
	if q.Join != nil {
		return query.Result{}, fault("invalid", "join requires a temporary relation")
	}
	r, err := d.catalog.Relation(q.Relation)
	if err != nil {
		return query.Result{}, invalid(err)
	}
	at := d.sequence
	var tx *Transaction
	if q.Transaction != "" {
		if q.At != nil {
			return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
		}
		tx, err = d.tx(q.Transaction)
		if err != nil {
			return query.Result{}, err
		}
		at = tx.Snapshot
	} else if q.At != nil {
		at = *q.At
	}
	if at < d.horizon || at > d.sequence {
		return query.Result{}, fault("snapshot", "snapshot unavailable; retained interval %d..%d", d.horizon, d.sequence)
	}
	if at < r.Created {
		return query.Result{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if err = query.Bind(r, q); err != nil {
		return query.Result{}, invalid(err)
	}
	if tx != nil && len(tx.writes[q.Relation]) > 0 {
		q.ForceScan = true
	}
	plan, keys := query.Choose(d.catalog, d.indexes, q, at, len(d.store.Tables[q.Relation]))
	rows := map[string]datum.Row{}
	if plan.Access == "index" {
		for _, key := range keys {
			if row, exists := d.store.Read(q.Relation, key, at); exists {
				rows[key] = row
			}
		}
	} else {
		rows = d.store.Scan(q.Relation, at)
	}
	if tx != nil {
		overlay(rows, tx.writes, q.Relation)
	}
	result, err := query.Execute(r, q, rows, plan, keys)
	d.queries++
	if err != nil {
		return result, invalid(err)
	}
	return result, nil
}

// queryTemporaryLocked serves a private relation. Temporary rows are copied
// into the result before the database lock is released, so the rows a caller
// receives stay valid even if the relation is dropped immediately afterwards.
func (d *DB) queryTemporaryLocked(q query.Request) (query.Result, error) {
	if q.Transaction == "" {
		return query.Result{}, fault("invalid", "temporary relation requires a transaction")
	}
	if q.At != nil {
		return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
	}
	tx, err := d.tx(q.Transaction)
	if err != nil {
		return query.Result{}, err
	}
	t, err := tx.temporaryRelation(q.Relation)
	if err != nil {
		return query.Result{}, err
	}
	if err = query.Bind(t.definition, q); err != nil {
		return query.Result{}, invalid(err)
	}
	rows := make(map[string]datum.Row, len(t.rows))
	for key, row := range t.rows {
		rows[key] = datum.Clone(row)
	}
	plan := query.Plan{Access: "scan", Candidates: len(rows), Snapshot: tx.Snapshot, Residuals: len(q.Where), Temporary: true}
	result, err := query.Execute(t.definition, q, rows, plan, nil)
	d.queries++
	if err != nil {
		return result, invalid(err)
	}
	return result, nil
}

// queryJoinLocked combines a transaction-private relation with one persistent
// relation. The persistent side is read at the transaction snapshot; temporary
// rows and persistent rows are never written to the same object. Rows without a
// matching persistent key are omitted, which keeps the result independent of
// later commits.
func (d *DB) queryJoinLocked(q query.Request) (query.Result, error) {
	if q.Transaction == "" {
		return query.Result{}, fault("invalid", "temporary relation requires a transaction")
	}
	if q.At != nil {
		return query.Result{}, fault("invalid", "at and transaction are mutually exclusive")
	}
	tx, err := d.tx(q.Transaction)
	if err != nil {
		return query.Result{}, err
	}
	t, err := tx.temporaryRelation(q.Relation)
	if err != nil {
		return query.Result{}, err
	}
	join := q.Join
	persistent, err := d.catalog.Relation(join.Relation)
	if err != nil {
		return query.Result{}, invalid(err)
	}
	if persistent.Created > tx.Snapshot {
		return query.Result{}, fault("snapshot", "relation did not exist at snapshot")
	}
	if join.Column == "" {
		join.Column = t.definition.Primary
	}
	column, ok := t.definition.Column(join.Column)
	if !ok {
		return query.Result{}, fault("invalid", "unknown join column %s", join.Column)
	}
	primary, _ := persistent.Column(persistent.Primary)
	if column.Type != primary.Type {
		return query.Result{}, fault("invalid", "join column type %s does not match %s primary key", column.Type, persistent.Name)
	}
	projected := join.Columns
	defaulted := len(projected) == 0
	if defaulted {
		projected = make([]string, 0, len(persistent.Columns))
		for _, c := range persistent.Columns {
			projected = append(projected, c.Name)
		}
	}
	combined := catalog.Relation{Name: t.definition.Name, Primary: t.definition.Primary, Created: tx.Snapshot}
	combined.Columns = append(combined.Columns, t.definition.Columns...)
	seen := map[string]bool{}
	for _, c := range t.definition.Columns {
		seen[c.Name] = true
	}
	for _, name := range projected {
		c, ok := persistent.Column(name)
		if !ok {
			return query.Result{}, fault("invalid", "unknown join projection %s", name)
		}
		if seen[c.Name] {
			// The default projection keeps the temporary column and skips the
			// same-named persistent one, so a natural key join needs no explicit
			// column list. An explicit collision is a caller mistake.
			if defaulted {
				continue
			}
			return query.Result{}, fault("invalid", "join projection %s collides with a temporary column", c.Name)
		}
		seen[c.Name] = true
		combined.Columns = append(combined.Columns, c)
	}
	if err = combined.Validate(); err != nil {
		return query.Result{}, invalid(err)
	}
	if err = query.Bind(combined, q); err != nil {
		return query.Result{}, invalid(err)
	}
	rows := make(map[string]datum.Row, len(t.rows))
	// The persistent side uses the same snapshot and private-write overlay as a
	// normal transactional read, so a join sees this transaction's own staged
	// persistent rows and never another transaction's uncommitted rows.
	persistentRows := d.store.Scan(persistent.Name, tx.Snapshot)
	overlay(persistentRows, tx.writes, persistent.Name)
	for key, temporaryRow := range t.rows {
		pk := temporaryRow[join.Column]
		if pk.Null() {
			continue
		}
		matched, exists := persistentRows[pk.Key()]
		if !exists {
			continue
		}
		combinedRow := datum.Clone(temporaryRow)
		for _, name := range projected {
			combinedRow[name] = matched[name]
		}
		rows[key] = combinedRow
	}
	plan := query.Plan{Access: "scan", Candidates: len(rows), Snapshot: tx.Snapshot, Residuals: len(q.Where), Temporary: true, Join: join.Relation, Joined: len(rows)}
	result, err := query.Execute(combined, q, rows, plan, nil)
	d.queries++
	if err != nil {
		return result, invalid(err)
	}
	return result, nil
}

func (d *DB) Get(id, table string, key datum.Value, temporary bool) (datum.Row, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	q := query.Request{Relation: table, Transaction: id, Limit: 1, Temporary: temporary}
	var r catalog.Relation
	if temporary {
		tx, err := d.tx(id)
		if err != nil {
			return nil, err
		}
		t, err := tx.temporaryRelation(table)
		if err != nil {
			return nil, err
		}
		r = t.definition
	} else {
		var err error
		r, err = d.catalog.Relation(table)
		if err != nil {
			return nil, invalid(err)
		}
	}
	col, _ := r.Column(r.Primary)
	if key.Null() || key.Kind != col.Type {
		return nil, fault("invalid", "invalid primary value")
	}
	q.Where = []query.Predicate{{Column: r.Primary, Op: "=", Value: key}}
	result, err := d.queryLocked(q)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return nil, fault("not_found", "tuple not found")
	}
	return result.Rows[0], nil
}
