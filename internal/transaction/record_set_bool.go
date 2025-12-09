package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type SetBoolLogRecord struct {
	LogRecord
	txNum    int64
	lsn      int64
	prevLSN  int64
	offset   int
	oldValue bool
	newValue bool
	block    *file.BlockID
}

// NewSetBoolLogRecord creates a new SetBoolLogRecord
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(1)] [newvalue(1)]
func NewSetBoolLogRecord(page *file.Page) *SetBoolLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt64Raw(txNumPos)

	lsnPos := txNumPos + 8
	lsn := page.GetInt64Raw(lsnPos)

	prevLSNPos := lsnPos + 8
	prevLSN := page.GetInt64Raw(prevLSNPos)

	fileNamePos := prevLSNPos + 8
	fileName := page.GetStringRaw(fileNamePos)

	blockNumPos := fileNamePos + 4 + len(fileName)
	blockNum := page.GetIntRaw(blockNumPos)

	offsetPos := blockNumPos + 4
	offset := page.GetIntRaw(offsetPos)

	oldValuePos := offsetPos + 4
	oldValue := page.GetBoolRaw(oldValuePos)

	newValuePos := oldValuePos + 1
	newValue := page.GetBoolRaw(newValuePos)

	block := file.NewBlockID(fileName, blockNum)

	return &SetBoolLogRecord{
		txNum:    txNum,
		lsn:      lsn,
		prevLSN:  prevLSN,
		offset:   offset,
		oldValue: oldValue,
		newValue: newValue,
		block:    block,
	}
}

// Op returns the operation type for this log record
func (s *SetBoolLogRecord) Op() LogRecordType {
	return LogRecordSetBool
}

// TxNumber returns the transaction number associated with this log record
func (s *SetBoolLogRecord) TxNumber() int64 {
	return s.txNum
}

// LSN returns the log sequence number for this record
func (s *SetBoolLogRecord) LSN() int64 {
	return s.lsn
}

// PrevLSN returns the previous log sequence number for this record
func (s *SetBoolLogRecord) PrevLSN() int64 {
	return s.prevLSN
}

// Block returns the block ID for this record
func (s *SetBoolLogRecord) Block() *file.BlockID {
	return s.block
}

// Offset returns the offset for this record
func (s *SetBoolLogRecord) Offset() int {
	return s.offset
}

// OldValue returns the old value for this record
func (s *SetBoolLogRecord) OldValue() bool {
	return s.oldValue
}

// NewValue returns the new value for this record
func (s *SetBoolLogRecord) NewValue() bool {
	return s.newValue
}

// Undo performs the undo operation for this log record
func (s *SetBoolLogRecord) Undo(tx *Transaction) error {
	// Restore the old value at the specified offset in the block
	// log=false because we don't want to log the undo operation itself
	return tx.SetBool(s.block, s.offset, s.oldValue, false)
}

// Redo performs the redo operation for this log record.
// Returns true if redo was performed, false if skipped (PageLSN >= record LSN).
func (s *SetBoolLogRecord) Redo(tx *Transaction) (bool, error) {
	pageLSN, err := tx.GetPageLSN(s.block)
	if err != nil {
		return false, err
	}
	if pageLSN >= s.lsn {
		return false, nil
	}

	err = tx.SetBool(s.block, s.offset, s.newValue, false)
	if err != nil {
		return false, err
	}

	return true, nil
}

// WriteSetBoolLogRecord writes a SetBoolLogRecord to the log manager
func WriteSetBoolLogRecord(lm *log.Manager, txNum int64, lsn int64, prevLSN int64, blk *file.BlockID, offset int, oldValue bool, newValue bool) error {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	fileNamePos := prevLSNPos + 8
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	newValuePos := oldValuePos + 1
	finalLen := newValuePos + 1

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordSetBool))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)
	page.SetStringRaw(fileNamePos, blk.Filename())
	page.SetIntRaw(blockNumPos, blk.Number())
	page.SetIntRaw(offsetPos, offset)
	page.SetBoolRaw(oldValuePos, oldValue)
	page.SetBoolRaw(newValuePos, newValue)

	return lm.Append(page.Bytes(), lsn)
}
