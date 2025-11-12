package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type SetIntLogRecord struct {
	LogRecord
	txNum    int
	offset   int
	oldValue int
	block    *file.BlockID
}

// NewSetIntLogRecord creates a new SetIntLogRecord
// Page format: [op(4)] [txNum(4)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(4)]
func NewSetIntLogRecord(page *file.Page) *SetIntLogRecord {
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
	oldValue := page.GetInt(oldValuePos)

	block := file.NewBlockID(fileName, blockNum)

	return &SetIntLogRecord{
		txNum:    txNum,
		offset:   offset,
		oldValue: oldValue,
		block:    block,
	}
}

// Op returns the operation type for this log record
func (s *SetIntLogRecord) Op() LogRecordType {
	return LogRecordSetInt
}

// TxNumber returns the transaction number associated with this log record
func (s *SetIntLogRecord) TxNumber() int {
	return s.txNum
}

// Undo performs the undo operation for this log record
func (s *SetIntLogRecord) Undo(tx *Transaction) error {
	// Restore the old value at the specified offset in the block
	// log=false because we don't want to log the undo operation itself
	return tx.SetInt(s.block, s.offset, s.oldValue, false)
}

// WriteSetIntLogRecord writes a SetIntLogRecord to the log manager
func WriteSetIntLogRecord(lm *log.Manager, txNum int, blk *file.BlockID, offset int, oldValue int) (int, error) {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	fileNamePos := txNumPos + 4
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	finalLen := oldValuePos + 4

	page := file.NewPage(finalLen)
	page.SetInt(opPos, int(LogRecordSetInt))
	page.SetInt(txNumPos, txNum)
	page.SetString(fileNamePos, blk.Filename())
	page.SetInt(blockNumPos, blk.Number())
	page.SetInt(offsetPos, offset)
	page.SetInt(oldValuePos, oldValue)

	return lm.Append(page.Bytes())
}
