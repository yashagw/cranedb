package record

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/transaction"
)

// TableScan provides an iterator interface for scanning through records in a table
type TableScan struct {
	transaction       *transaction.Transaction
	layout            *Layout
	fileName          string
	currentRecordPage *RecordPage
	currentSlot       int
}

// NewTableScan creates a new table scanner for the given table
func NewTableScan(transaction *transaction.Transaction, layout *Layout, tableName string) *TableScan {
	fileName := tableName + ".tbl"

	ts := &TableScan{
		transaction: transaction,
		layout:      layout,
		fileName:    fileName,
	}

	if numBlocks, err := transaction.Size(fileName); err != nil {
		panic(err)
	} else if numBlocks == 0 {
		ts.MoveToNewBlock()
	} else {
		ts.MoveToBlock(0)
	}

	return ts
}

// Close unpins the current record page
func (ts *TableScan) Close() {
	if ts.currentRecordPage != nil {
		ts.transaction.Unpin(ts.currentRecordPage.Block())
	}
}

// BeforeFirst positions the scanner before the first record
func (ts *TableScan) BeforeFirst() {
	ts.MoveToBlock(0)
}

// Next moves to the next record and returns true if successful
func (ts *TableScan) Next() bool {
	ts.currentSlot = ts.currentRecordPage.NextUsedSlot(ts.currentSlot)
	if ts.currentSlot == -1 {
		if ts.AtLastBlock() {
			return false
		}
		ts.MoveToBlock(ts.currentRecordPage.Block().Number() + 1)
		ts.currentSlot = ts.currentRecordPage.NextUsedSlot(ts.currentSlot)
	}
	return true
}

// Insert positions the scanner for inserting a new record
func (ts *TableScan) Insert() {
	ts.currentSlot = ts.currentRecordPage.InsertSlot(ts.currentSlot)
	for ts.currentSlot == -1 {
		if ts.AtLastBlock() {
			ts.MoveToNewBlock()
		} else {
			ts.MoveToBlock(ts.currentRecordPage.Block().Number() + 1)
		}
		ts.currentSlot = ts.currentRecordPage.InsertSlot(ts.currentSlot)
	}
}

// Delete removes the current record
func (ts *TableScan) Delete() {
	ts.currentRecordPage.Delete(ts.currentSlot)
}

// MoveToBlock moves the scanner to the specified block
func (ts *TableScan) MoveToBlock(block int) {
	ts.Close()
	blockID := file.NewBlockID(ts.fileName, block)
	ts.currentRecordPage = NewRecordPage(ts.transaction, blockID, ts.layout)
	ts.currentSlot = -1
}

// MoveToNewBlock creates a new block and moves the scanner to it
func (ts *TableScan) MoveToNewBlock() {
	ts.Close()
	blockID, err := ts.transaction.Append(ts.fileName)
	if err != nil {
		panic(err)
	}
	ts.currentRecordPage = NewRecordPage(ts.transaction, blockID, ts.layout)
	ts.currentRecordPage.Format()
	ts.currentSlot = -1
}

// AtLastBlock returns true if the scanner is at the last block
func (ts *TableScan) AtLastBlock() bool {
	if numBlocks, err := ts.transaction.Size(ts.fileName); err != nil {
		panic(err)
	} else {
		return ts.currentRecordPage.Block().Number() == numBlocks-1
	}
}

// MoveToRID moves the scanner to the record with the specified RID
func (ts *TableScan) MoveToRID(rid *RID) {
	ts.Close()
	blockID := file.NewBlockID(ts.fileName, rid.Block())
	ts.currentRecordPage = NewRecordPage(ts.transaction, blockID, ts.layout)
	ts.currentSlot = rid.Slot()
}

// GetRID returns the RID of the current record
func (ts *TableScan) GetRID() *RID {
	return NewRID(ts.currentRecordPage.Block().Number(), ts.currentSlot)
}

// GetInt retrieves an integer value from the current record
func (ts *TableScan) GetInt(fieldName string) int {
	return ts.currentRecordPage.GetInt(ts.currentSlot, fieldName)
}

// GetString retrieves a string value from the current record
func (ts *TableScan) GetString(fieldName string) string {
	return ts.currentRecordPage.GetString(ts.currentSlot, fieldName)
}

// SetInt sets an integer value in the current record
func (ts *TableScan) SetInt(fieldName string, value int) {
	ts.currentRecordPage.SetInt(ts.currentSlot, fieldName, value)
}

// SetString sets a string value in the current record
func (ts *TableScan) SetString(fieldName string, value string) {
	ts.currentRecordPage.SetString(ts.currentSlot, fieldName, value)
}
