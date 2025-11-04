package scan

import "github.com/yashagw/cranedb/internal/record"

// Scan is the fundamental interface for iterating over records in a scan operation.
// It provides basic methods to position, navigate, retrieve field values, and check fields.
type Scan interface {
	// BeforeFirst positions the scan before the first record.
	BeforeFirst()
	// Next moves the scan to the next record. Returns true if there is a next record.
	Next() bool
	// GetInt returns the value of the specified integer field from the current record.
	GetInt(fldname string) int
	// GetString returns the value of the specified string field from the current record.
	GetString(fldname string) string
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
	SetInt(fldname string, val int)
	// SetString sets the value of the specified string field in the current record.
	SetString(fldname string, val string)
	// Insert inserts a new record in the scan.
	Insert()
	// Delete removes the current record from the scan.
	Delete()
	// GetRid returns the record identifier (RID) of the current record.
	GetRid() *record.RID
	// MoveToRid moves the scan to the record specified by the given RID.
	MoveToRid(rid *record.RID)
}
