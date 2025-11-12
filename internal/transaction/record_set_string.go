package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type SetStringLogRecord struct {
	LogRecord
	txNum    int
	offset   int
	oldValue string
	block    *file.BlockID
}

// NewSetStringLogRecord creates a new SetStringLogRecord
// Page format: [op(4)] [txNum(4)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(4+len(oldvalue))]
func NewSetStringLogRecord(page *file.Page) *SetStringLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt(txNumPos)

	fileNamePos := txNumPos + 4
	fileName := page.GetString(fileNamePos)

	blockNumPos := fileNamePos + 4 + len(fileName)
	blockNum := page.GetInt(blockNumPos)

	offsetPos := blockNumPos + 4
	offset := page.GetInt(offsetPos)

	oldvaluePos := offsetPos + 4
	oldValue := page.GetString(oldvaluePos)

	block := file.NewBlockID(fileName, blockNum)

	return &SetStringLogRecord{
		txNum:    txNum,
		offset:   offset,
		oldValue: oldValue,
		block:    block,
	}
}

// Op returns the operation type for this log record
func (s *SetStringLogRecord) Op() LogRecordType {
	return LogRecordSetString
}

// TxNumber returns the transaction number associated with this log record
func (s *SetStringLogRecord) TxNumber() int {
	return s.txNum
}

// Undo performs the undo operation for this log record
func (s *SetStringLogRecord) Undo(tx *Transaction) error {
	// Restore the old value at the specified offset in the block
	// log=false because we don't want to log the undo operation itself
	return tx.SetString(s.block, s.offset, s.oldValue, false)
}

// WriteSetStringLogRecord writes a SetStringLogRecord to the log manager
func WriteSetStringLogRecord(lm *log.Manager, txNum int, blk *file.BlockID, offset int, oldValue string) (int, error) {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	fileNamePos := txNumPos + 4
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	finalLen := oldValuePos + 4 + len(oldValue)

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordSetString))
	page.SetInt(txNumPos, txNum)
	page.SetString(fileNamePos, blk.Filename())
	page.SetInt(blockNumPos, blk.Number())
	page.SetInt(offsetPos, offset)
	page.SetString(oldValuePos, oldValue)

	return lm.Append(page.Bytes())
}
