package scan

import (
	"fmt"
	"log"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// TableScan provides an iterator interface for scanning through records in a table
type TableScan struct {
	transaction       *transaction.Transaction
	layout            *record.Layout
	fileName          string
	currentRecordPage *record.RecordPage
	currentSlot       int
}

// NewTableScan creates a new table scanner for the given table
func NewTableScan(transaction *transaction.Transaction, layout *record.Layout, tableName string) (*TableScan, error) {
	fileName := tableName + ".tbl"

	ts := &TableScan{
		transaction: transaction,
		layout:      layout,
		fileName:    fileName,
	}

	if numBlocks, err := transaction.Size(fileName); err != nil {
		return nil, err
	} else if numBlocks == 0 {
		err := ts.MoveToNewBlock()
		if err != nil {
			return nil, err
		}
	} else {
		err := ts.MoveToBlock(0)
		if err != nil {
			return nil, err
		}
	}

	return ts, nil
}

// Close unpins the current record page
func (ts *TableScan) Close() {
	if ts.currentRecordPage != nil {
		ts.transaction.Unpin(ts.currentRecordPage.Block())
	}
}

// HasField checks if the table scan has the specified field.
func (ts *TableScan) HasField(fieldName string) bool {
	return ts.layout.GetSchema().HasField(fieldName)
}

// BeforeFirst positions the scanner before the first record
func (ts *TableScan) BeforeFirst() error {
	return ts.MoveToBlock(0)
}

// Next moves to the next record and returns true if successful
func (ts *TableScan) Next() (bool, error) {
	nextSlot, err := ts.currentRecordPage.NextUsedSlot(ts.currentSlot)
	if err != nil {
		return false, err
	}
	ts.currentSlot = nextSlot
	for ts.currentSlot == -1 {
		if atLastBlock, err := ts.AtLastBlock(); err != nil {
			return false, err
		} else if atLastBlock {
			return false, nil
		}
		err := ts.MoveToBlock(ts.currentRecordPage.Block().Number() + 1)
		if err != nil {
			return false, err
		}
		nextSlot, err = ts.currentRecordPage.NextUsedSlot(ts.currentSlot)
		if err != nil {
			return false, err
		}
		ts.currentSlot = nextSlot
	}
	return true, nil
}

// Insert inserts a new record somewhere in the scan and moves the scan to the new record.
// If there is no room in the current block, it moves to the next block.
// If there are no more blocks, it creates a new block.
func (ts *TableScan) Insert() error {
	// Try to insert in the current block
	newSlot, err := ts.currentRecordPage.InsertSlot(ts.currentSlot)
	if err != nil {
		log.Printf("[INSERT] InsertSlot failed: %v", err)
		return err
	}
	ts.currentSlot = newSlot

	// If no room in current block, try next blocks
	for ts.currentSlot == -1 {
		atLastBlock, err := ts.AtLastBlock()
		if err != nil {
			log.Printf("[INSERT] AtLastBlock failed: %v", err)
			return err
		}

		createdNewBlock := false
		if atLastBlock {
			// No more blocks, create a new one
			log.Printf("[INSERT] No room in block %d and at last block, creating new block for %s", ts.currentRecordPage.Block().Number(), ts.fileName)
			err = ts.MoveToNewBlock()
			if err != nil {
				log.Printf("[INSERT] MoveToNewBlock failed: %v", err)
				return err
			}
			createdNewBlock = true
		} else {
			// Move to the next block
			nextBlockNum := ts.currentRecordPage.Block().Number() + 1
			log.Printf("[INSERT] No room in block %d, moving to next block %d", ts.currentRecordPage.Block().Number(), nextBlockNum)
			err = ts.MoveToBlock(nextBlockNum)
			if err != nil {
				log.Printf("[INSERT] MoveToBlock failed: %v", err)
				return err
			}
		}

		// Try to insert in the new/next block
		newSlot, err = ts.currentRecordPage.InsertSlot(ts.currentSlot)
		if err != nil {
			log.Printf("[INSERT] InsertSlot failed: %v", err)
			return err
		}
		ts.currentSlot = newSlot

		// A newly formatted block should always have empty slots
		if ts.currentSlot == -1 && createdNewBlock {
			return fmt.Errorf("newly formatted block has no empty slots - possible layout issue")
		}
	}

	log.Printf("[INSERT] Found slot %d in block %d for %s", ts.currentSlot, ts.currentRecordPage.Block().Number(), ts.fileName)
	return nil
}

// Delete removes the current record
func (ts *TableScan) Delete() error {
	return ts.currentRecordPage.Delete(ts.currentSlot)
}

// MoveToBlock moves the scanner to the specified block
func (ts *TableScan) MoveToBlock(block int) error {
	ts.Close()
	blockID := file.NewBlockID(ts.fileName, block)
	recordPage, err := record.NewRecordPage(ts.transaction, blockID, ts.layout)
	if err != nil {
		return err
	}
	ts.currentRecordPage = recordPage
	ts.currentSlot = -1
	return nil
}

// MoveToNewBlock creates a new block and moves the scanner to it
func (ts *TableScan) MoveToNewBlock() error {
	ts.Close()
	blockID, err := ts.transaction.Append(ts.fileName)
	if err != nil {
		return err
	}
	recordPage, err := record.NewRecordPage(ts.transaction, blockID, ts.layout)
	if err != nil {
		return err
	}
	ts.currentRecordPage = recordPage
	err = ts.currentRecordPage.Format()
	if err != nil {
		return err
	}
	ts.currentSlot = -1
	return nil
}

// AtLastBlock returns true if the scanner is at the last block
func (ts *TableScan) AtLastBlock() (bool, error) {
	if numBlocks, err := ts.transaction.Size(ts.fileName); err != nil {
		return false, err
	} else {
		return ts.currentRecordPage.Block().Number() == numBlocks-1, nil
	}
}

// MoveToRID moves the scanner to the record with the specified RID
func (ts *TableScan) MoveToRID(rid *record.RID) error {
	ts.Close()
	blockID := file.NewBlockID(ts.fileName, rid.Block())
	recordPage, err := record.NewRecordPage(ts.transaction, blockID, ts.layout)
	if err != nil {
		return err
	}
	ts.currentRecordPage = recordPage
	ts.currentSlot = rid.Slot()
	return nil
}

// GetRID returns the RID of the current record
func (ts *TableScan) GetRID() (*record.RID, error) {
	if ts.currentSlot < 0 {
		return nil, fmt.Errorf("invalid slot %d for GetRID", ts.currentSlot)
	}
	return record.NewRID(ts.currentRecordPage.Block().Number(), ts.currentSlot), nil
}

// GetInt retrieves an integer value from the current record
func (ts *TableScan) GetInt(fieldName string) (int, error) {
	if ts.currentSlot < 0 {
		return 0, fmt.Errorf("attempted to GetInt on invalid slot %d", ts.currentSlot)
	}
	return ts.currentRecordPage.GetInt(ts.currentSlot, fieldName)
}

// GetString retrieves a string value from the current record
func (ts *TableScan) GetString(fieldName string) (string, error) {
	if ts.currentSlot < 0 {
		return "", fmt.Errorf("attempted to GetString on invalid slot %d", ts.currentSlot)
	}
	return ts.currentRecordPage.GetString(ts.currentSlot, fieldName)
}

// GetValue retrieves a value from the current record as an interface{}
func (ts *TableScan) GetValue(fieldName string) (any, error) {
	fieldType := ts.layout.GetSchema().Type(fieldName)
	if fieldType == "int" {
		return ts.GetInt(fieldName)
	}
	return ts.GetString(fieldName)
}

// SetInt sets an integer value in the current record
func (ts *TableScan) SetInt(fieldName string, value int) error {
	return ts.currentRecordPage.SetInt(ts.currentSlot, fieldName, value)
}

// SetString sets a string value in the current record
func (ts *TableScan) SetString(fieldName string, value string) error {
	return ts.currentRecordPage.SetString(ts.currentSlot, fieldName, value)
}
