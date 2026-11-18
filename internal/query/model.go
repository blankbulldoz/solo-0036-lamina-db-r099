package query

import "lamina/internal/datum"

type Predicate struct {
	Column string      `json:"column"`
	Op     string      `json:"op"`
	Value  datum.Value `json:"value"`
}
type Ordering struct {
	Column string `json:"column"`
	Desc   bool   `json:"desc"`
}
type Aggregate struct {
	Function string `json:"function"`
	Column   string `json:"column,omitempty"`
}
type Request struct {
	Relation    string      `json:"relation"`
	Columns     []string    `json:"columns,omitempty"`
	Where       []Predicate `json:"where,omitempty"`
	Order       []Ordering  `json:"order,omitempty"`
	Limit       int         `json:"limit,omitempty"`
	At          *uint64     `json:"at,omitempty"`
	Transaction string      `json:"transaction,omitempty"`
	Aggregate   *Aggregate  `json:"aggregate,omitempty"`
	ForceScan   bool        `json:"force_scan,omitempty"`
	Temporary   bool        `json:"temporary,omitempty"`
	Join        *Join       `json:"join,omitempty"`
}

// Join combines a transaction-private relation with one persistent relation.
// The private column is matched against the persistent primary key at the
// transaction's snapshot; the persistent row is only read, never written.
type Join struct {
	Relation string   `json:"relation"`
	Column   string   `json:"column"`
	Columns  []string `json:"columns"`
}
type Plan struct {
	Access     string `json:"access"`
	Index      string `json:"index,omitempty"`
	Candidates int    `json:"candidates"`
	Snapshot   uint64 `json:"snapshot"`
	Residuals  int    `json:"residuals"`
	Temporary  bool   `json:"temporary,omitempty"`
	Join       string `json:"join,omitempty"`
	Joined     int    `json:"joined,omitempty"`
}
type Result struct {
	Rows      []datum.Row `json:"rows"`
	Plan      Plan        `json:"plan"`
	Scanned   int         `json:"scanned"`
	Matched   int         `json:"matched"`
	Truncated bool        `json:"truncated"`
}
