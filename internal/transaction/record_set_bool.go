package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type SetBoolLogRecord struct {
	LogRecord
	txNum    int
	offset   int
	oldValue bool
	block    *file.BlockID
}

// NewSetBoolLogRecord creates a new SetBoolLogRecord
// Page format: [op(4)] [txNum(4)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(1)]
func NewSetBoolLogRecord(page *file.Page) *SetBoolLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt(txNumPos)

	fileNamePos := txNumPos + 4
	fileName := page.GetString(fileNamePos)

	blockNumPos := fileNamePos + 4 + len(fileName)
	blockNum := page.GetInt(blockNumPos)

	offsetPos := blockNumPos + 4
	offset := page.GetInt(offsetPos)

	oldValuePos := offsetPos + 4
	oldValue := page.GetBool(oldValuePos)

	block := file.NewBlockID(fileName, blockNum)

	return &SetBoolLogRecord{
		txNum:    txNum,
		offset:   offset,
		oldValue: oldValue,
		block:    block,
	}
}

// Op returns the operation type for this log record
func (s *SetBoolLogRecord) Op() LogRecordType {
	return LogRecordSetBool
}

// TxNumber returns the transaction number associated with this log record
func (s *SetBoolLogRecord) TxNumber() int {
	return s.txNum
}

// Undo performs the undo operation for this log record
func (s *SetBoolLogRecord) Undo(tx *Transaction) error {
	// Restore the old value at the specified offset in the block
	// log=false because we don't want to log the undo operation itself
	return tx.SetBool(s.block, s.offset, s.oldValue, false)
}

// WriteSetBoolLogRecord writes a SetBoolLogRecord to the log manager
func WriteSetBoolLogRecord(lm *log.Manager, txNum int, blk *file.BlockID, offset int, oldValue bool) (int, error) {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	fileNamePos := txNumPos + 4
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	finalLen := oldValuePos + 1

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordSetBool))
	page.SetInt(txNumPos, txNum)
	page.SetString(fileNamePos, blk.Filename())
	page.SetInt(blockNumPos, blk.Number())
	page.SetInt(offsetPos, offset)
	page.SetBool(oldValuePos, oldValue)

	return lm.Append(page.Bytes())
}
