package engine

import (
	"sort"

	"lamina/internal/catalog"
	"lamina/internal/datum"
)

// Temporary relations live only inside one transaction. They never enter the
// persistent catalog, version store or journal, so naming a temporary relation
// after any persistent relation cannot redirect a persistent write.
const (
	MaxTemporaryRelationsPerTransaction = 8
	MaxTemporaryRowsPerTransaction      = 1000
	MaxTemporaryBytesPerTransaction     = 4 << 20
	MaxTemporaryRelations               = 128
	MaxTemporaryRows                    = 32768
	MaxTemporaryBytes                   = 32 << 20
)

type temporaryRelation struct {
	definition catalog.Relation
	rows       map[string]datum.Row
	bytes      int
}

// TemporaryRelation is the observable description of a private relation.
type TemporaryRelation struct {
	catalog.Relation
	Rows  int `json:"rows"`
	Bytes int `json:"bytes"`
}

func (tx *Transaction) temporaryRelation(name string) (*temporaryRelation, error) {
	t, ok := tx.temporary[name]
	if !ok {
		return nil, fault("not_found", "temporary relation %s is not active in this transaction", name)
	}
	return t, nil
}

func (tx *Transaction) temporaryUsage() (relations, rows, bytes int) {
	relations = len(tx.temporary)
	for _, t := range tx.temporary {
		rows += len(t.rows)
		bytes += t.bytes
	}
	return relations, rows, bytes
}

// temporaryUsage reports the database-wide temporary footprint. The counters
// are maintained on every mutation so capacity checks stay O(1).
func (d *DB) temporaryUsage() (relations, rows, bytes int) {
	return d.temporaryRelations, d.temporaryRows, d.temporaryBytes
}

func (d *DB) addTemporaryUsage(relations, rows, bytes int) {
	d.temporaryRelations += relations
	d.temporaryRows += rows
	d.temporaryBytes += bytes
}

// releaseTemporary subtracts a transaction's private footprint and every
// relation it owns. It is idempotent with respect to the transaction map: the
// caller removes the transaction afterwards. Other transactions keep their own
// relations and are never touched by another session's completion.
func (d *DB) releaseTemporary(tx *Transaction) {
	relations, rows, bytes := tx.temporaryUsage()
	d.addTemporaryUsage(-relations, -rows, -bytes)
	tx.temporary = map[string]*temporaryRelation{}
}

func temporaryRowBytes(row datum.Row) int {
	size := 0
	for _, v := range row {
		size += 16
		if v.Kind == "text" {
			size += len(v.Text)
		}
	}
	return size
}

// CreateTemporary registers a relation visible only to its owning transaction.
// The name is resolved in the private namespace only: a temporary relation may
// share a name with a persistent relation or with another transaction's
// temporary relation without any resolution ambiguity.
func (d *DB) CreateTemporary(id string, definition catalog.Relation) (catalog.Relation, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return definition, err
	}
	tx, err := d.tx(id)
	if err != nil {
		return definition, err
	}
	if tx.ReadOnly {
		return definition, fault("conflict", "read-only transaction")
	}
	if err := definition.Validate(); err != nil {
		return definition, invalid(err)
	}
	if _, exists := tx.temporary[definition.Name]; exists {
		return definition, fault("conflict", "temporary relation %s already exists in this transaction", definition.Name)
	}
	if len(tx.temporary) >= MaxTemporaryRelationsPerTransaction {
		return definition, fault("capacity", "temporary relation limit %d per transaction", MaxTemporaryRelationsPerTransaction)
	}
	if d.temporaryRelations >= MaxTemporaryRelations {
		return definition, fault("capacity", "database temporary relation limit %d", MaxTemporaryRelations)
	}
	// Created records when the private relation became visible to its owner. It
	// is not a persistent identity: it is never journaled, imaged or reversible
	// into the catalog.
	definition.Created = tx.Snapshot
	definition.Columns = append([]catalog.Column(nil), definition.Columns...)
	tx.temporary[definition.Name] = &temporaryRelation{definition: definition, rows: map[string]datum.Row{}}
	d.addTemporaryUsage(1, 0, 0)
	return definition, nil
}

// DropTemporary releases one private relation immediately. The database lock
// guarantees no query can be inside a read of this relation at this point; any
// result already returned holds copied rows.
func (d *DB) DropTemporary(id, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.usable(); err != nil {
		return err
	}
	tx, err := d.tx(id)
	if err != nil {
		return err
	}
	t, ok := tx.temporary[name]
	if !ok {
		return fault("not_found", "temporary relation %s is not active in this transaction", name)
	}
	d.addTemporaryUsage(-1, -len(t.rows), -t.bytes)
	delete(tx.temporary, name)
	return nil
}

func (d *DB) ListTemporary(id string) ([]TemporaryRelation, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return nil, err
	}
	tx, ok := d.transactions[id]
	if !ok {
		return nil, fault("not_found", "transaction not active")
	}
	out := make([]TemporaryRelation, 0, len(tx.temporary))
	for _, t := range tx.temporary {
		r := t.definition
		r.Columns = append([]catalog.Column(nil), r.Columns...)
		out = append(out, TemporaryRelation{Relation: r, Rows: len(t.rows), Bytes: t.bytes})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (d *DB) putTemporary(tx *Transaction, name string, row datum.Row, insertOnly bool) error {
	t, err := tx.temporaryRelation(name)
	if err != nil {
		return err
	}
	normalized, err := t.definition.Normalize(row)
	if err != nil {
		return invalid(err)
	}
	key := t.definition.Key(normalized)
	old, exists := t.rows[key]
	if insertOnly && exists {
		return fault("conflict", "primary already exists in temporary relation")
	}
	oldBytes := 0
	if exists {
		oldBytes = temporaryRowBytes(old)
	}
	newBytes := temporaryRowBytes(normalized)
	_, txRows, txBytes := tx.temporaryUsage()
	if !exists && txRows >= MaxTemporaryRowsPerTransaction {
		return fault("capacity", "temporary row limit %d per transaction", MaxTemporaryRowsPerTransaction)
	}
	if txBytes-oldBytes+newBytes > MaxTemporaryBytesPerTransaction {
		return fault("capacity", "temporary byte limit %d per transaction", MaxTemporaryBytesPerTransaction)
	}
	if !exists && d.temporaryRows >= MaxTemporaryRows {
		return fault("capacity", "database temporary row limit %d", MaxTemporaryRows)
	}
	if d.temporaryBytes-oldBytes+newBytes > MaxTemporaryBytes {
		return fault("capacity", "database temporary byte limit %d", MaxTemporaryBytes)
	}
	t.rows[key] = normalized
	t.bytes += newBytes - oldBytes
	added := 0
	if !exists {
		added = 1
	}
	d.addTemporaryUsage(0, added, newBytes-oldBytes)
	return nil
}

func (d *DB) deleteTemporary(tx *Transaction, name string, value datum.Value) error {
	t, err := tx.temporaryRelation(name)
	if err != nil {
		return err
	}
	col, _ := t.definition.Column(t.definition.Primary)
	if value.Null() || value.Kind != col.Type {
		return fault("invalid", "invalid primary value")
	}
	key := value.Key()
	if old, exists := t.rows[key]; exists {
		size := temporaryRowBytes(old)
		t.bytes -= size
		delete(t.rows, key)
		d.addTemporaryUsage(0, -1, -size)
	}
	return nil
}
