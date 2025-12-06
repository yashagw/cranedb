package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type SetIntLogRecord struct {
	LogRecord
	txNum    int64
	lsn      int64
	prevLSN  int64
	offset   int
	oldValue int
	newValue int
	block    *file.BlockID
}

// NewSetIntLogRecord creates a new SetIntLogRecord
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(4)] [newvalue(4)]
func NewSetIntLogRecord(page *file.Page) *SetIntLogRecord {
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
	oldValue := page.GetIntRaw(oldValuePos)

	newValuePos := oldValuePos + 4
	newValue := page.GetIntRaw(newValuePos)

	block := file.NewBlockID(fileName, blockNum)

	return &SetIntLogRecord{
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
func (s *SetIntLogRecord) Op() LogRecordType {
	return LogRecordSetInt
}

// TxNumber returns the transaction number associated with this log record
func (s *SetIntLogRecord) TxNumber() int64 {
	return s.txNum
}

// LSN returns the log sequence number for this record
func (s *SetIntLogRecord) LSN() int64 {
	return s.lsn
}

// PrevLSN returns the previous log sequence number for this record
func (s *SetIntLogRecord) PrevLSN() int64 {
	return s.prevLSN
}

// Block returns the block ID for this record
func (s *SetIntLogRecord) Block() *file.BlockID {
	return s.block
}

// Offset returns the offset for this record
func (s *SetIntLogRecord) Offset() int {
	return s.offset
}

// OldValue returns the old value for this record
func (s *SetIntLogRecord) OldValue() int {
	return s.oldValue
}

// NewValue returns the new value for this record
func (s *SetIntLogRecord) NewValue() int {
	return s.newValue
}

// Undo performs the undo operation for this log record
func (s *SetIntLogRecord) Undo(tx *Transaction) error {
	// Restore the old value at the specified offset in the block
	// log=false because we don't want to log the undo operation itself
	return tx.SetInt(s.block, s.offset, s.oldValue, false)
}

// WriteSetIntLogRecord writes a SetIntLogRecord to the log manager
func WriteSetIntLogRecord(lm *log.Manager, txNum int64, lsn int64, prevLSN int64, blk *file.BlockID, offset int, oldValue int, newValue int) error {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	fileNamePos := prevLSNPos + 8
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	newValuePos := oldValuePos + 4
	finalLen := newValuePos + 4

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordSetInt))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)
	page.SetStringRaw(fileNamePos, blk.Filename())
	page.SetIntRaw(blockNumPos, blk.Number())
	page.SetIntRaw(offsetPos, offset)
	page.SetIntRaw(oldValuePos, oldValue)
	page.SetIntRaw(newValuePos, newValue)

	return lm.Append(page.Bytes(), lsn)
}
