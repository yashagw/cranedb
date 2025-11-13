package index

import (
	"github.com/yashagw/cranedb/internal/record"
)

// Index is the interface for index operations.
// This interface is in a separate package to avoid import cycles.
type Index interface {
	// BeforeFirst positions the index before the first record matching the given search key.
	BeforeFirst(searchKey any) error
	// Next moves to the next record with the same search key.
	// Returns false if there are no more matching records.
	Next() (bool, error)
	// GetDataRid returns the record identifier (RID) of the current record.
	GetDataRid() (*record.RID, error)
	// Insert inserts a new record into the index with the given data value and record identifier.
	Insert(dataVal any, dataRid *record.RID) error
	// Delete deletes a record from the index with the given data value and record identifier.
	Delete(dataVal any, dataRid *record.RID) error
	// Close closes the index.
	Close() error
}
