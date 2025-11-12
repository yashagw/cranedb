package scan

import (
	"github.com/yashagw/cranedb/internal/record"
)

// Scan is the fundamental interface for iterating over records in a scan operation.
// It provides basic methods to position, navigate, retrieve field values, and check fields.
type Scan interface {
	// BeforeFirst positions the scan before the first record.
	BeforeFirst() error
	// Next moves the scan to the next record. Returns true if there is a next record.
	Next() (bool, error)
	// GetInt returns the value of the specified integer field from the current record.
	GetInt(fldname string) (int, error)
	// GetString returns the value of the specified string field from the current record.
	GetString(fldname string) (string, error)
	// GetValue returns the value of the specified field from the current record.
	GetValue(fldname string) (any, error)
	// HasField checks if the scan contains the specified field.
	HasField(fldname string) bool
	// Close releases the resources held by the scan.
	Close()
}

// UpdateScan is an extension of the Scan interface that allows updates to the underlying records.
// It provides additional methods for modifying, inserting, and deleting records,
// as well as navigating using record identifiers.
type UpdateScan interface {
	Scan

	// SetInt sets the value of the specified integer field in the current record.
	SetInt(fldname string, val int) error
	// SetString sets the value of the specified string field in the current record.
	SetString(fldname string, val string) error
	// Insert inserts a new record in the scan.
	Insert() error
	// Delete removes the current record from the scan.
	Delete() error
	// GetRID returns the record identifier (RID) of the current record.
	GetRID() (*record.RID, error)
	// MoveToRID moves the scan to the record specified by the given RID.
	MoveToRID(rid *record.RID) error
}

// Predicate is an interface for checking if a predicate is satisfied by a scan.
// This allows the scan package to work with predicates without importing the query package,
// breaking the import cycle between scan and query.
type Predicate interface {
	// IsSatisfied checks if the predicate is satisfied by the current record in the scan.
	IsSatisfied(s Scan) (bool, error)
}
