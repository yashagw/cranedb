package temptable

import (
	"fmt"
	"sync"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	nextTableNumMutex sync.Mutex
	nextTableNum      int
)

// TempTable represents a temporary table used during query execution
type TempTable struct {
	tx      *transaction.Transaction
	tblname string
	layout  *record.Layout
}

// NewTempTable creates a new temporary table with the given transaction and schema
func NewTempTable(tx *transaction.Transaction, sch *record.Schema) *TempTable {
	tblname := nextTableName()
	layout := record.NewLayoutFromSchema(sch)

	return &TempTable{
		tx:      tx,
		tblname: tblname,
		layout:  layout,
	}
}

// Open returns an UpdateScan for the temporary table
func (tt *TempTable) Open() (scan.UpdateScan, error) {
	return table.NewTableScan(tt.tx, tt.layout, tt.tblname)
}

// TableName returns the name of the temporary table
func (tt *TempTable) TableName() string {
	return tt.tblname
}

// GetLayout returns the layout of the temporary table
func (tt *TempTable) GetLayout() *record.Layout {
	return tt.layout
}

// nextTableName returns the next unique temporary table name in a thread-safe manner
func nextTableName() string {
	nextTableNumMutex.Lock()
	defer nextTableNumMutex.Unlock()
	nextTableNum++
	return fmt.Sprintf("temp%d", nextTableNum)
}
