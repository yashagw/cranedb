package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// CLRLogRecord represents a Compensation Log Record in ARIES
// CLRs are written during undo operations to prevent re-undoing the same operation
type CLRLogRecord struct {
	LogRecord
	txNum       int64
	lsn         int64
	prevLSN     int64
	undoNextLSN int64         // Points to the next log record to undo for this transaction
	originalOp  LogRecordType // The original operation being undone (SetInt, SetString, SetBool)
	block       *file.BlockID
	offset      int
	// Value to restore (union type - only one will be used based on originalOp)
	intValue    int
	stringValue string
	boolValue   bool
}

// NewCLRLogRecord creates a new CLRLogRecord from a page
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [undoNextLSN(8)] [originalOp(4)] [filename(4+len)] [blockNum(4)] [offset(4)] [value...]
func NewCLRLogRecord(page *file.Page) *CLRLogRecord {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	txNum := page.GetInt64Raw(txNumPos)

	lsnPos := txNumPos + 8
	lsn := page.GetInt64Raw(lsnPos)

	prevLSNPos := lsnPos + 8
	prevLSN := page.GetInt64Raw(prevLSNPos)

	undoNextLSNPos := prevLSNPos + 8
	undoNextLSN := page.GetInt64Raw(undoNextLSNPos)

	originalOpPos := undoNextLSNPos + 8
	originalOp := LogRecordType(page.GetIntRaw(originalOpPos))

	fileNamePos := originalOpPos + 4
	fileName := page.GetStringRaw(fileNamePos)

	blockNumPos := fileNamePos + 4 + len(fileName)
	blockNum := page.GetIntRaw(blockNumPos)

	offsetPos := blockNumPos + 4
	offset := page.GetIntRaw(offsetPos)

	block := file.NewBlockID(fileName, blockNum)

	clr := &CLRLogRecord{
		txNum:       txNum,
		lsn:         lsn,
		prevLSN:     prevLSN,
		undoNextLSN: undoNextLSN,
		originalOp:  originalOp,
		block:       block,
		offset:      offset,
	}

	// Read the value based on the original operation type
	valuePos := offsetPos + 4
	switch originalOp {
	case LogRecordSetInt:
		clr.intValue = page.GetIntRaw(valuePos)
	case LogRecordSetString:
		clr.stringValue = page.GetStringRaw(valuePos)
	case LogRecordSetBool:
		clr.boolValue = page.GetBoolRaw(valuePos)
	}

	return clr
}

// Op returns the operation type for this log record
func (c *CLRLogRecord) Op() LogRecordType {
	return LogRecordCLR
}

// TxNumber returns the transaction number associated with this log record
func (c *CLRLogRecord) TxNumber() int64 {
	return c.txNum
}

// LSN returns the log sequence number for this record
func (c *CLRLogRecord) LSN() int64 {
	return c.lsn
}

// PrevLSN returns the previous log sequence number for this record
func (c *CLRLogRecord) PrevLSN() int64 {
	return c.prevLSN
}

// UndoNextLSN returns the LSN of the next log record to undo for this transaction
func (c *CLRLogRecord) UndoNextLSN() int64 {
	return c.undoNextLSN
}

// OriginalOp returns the type of the original operation being undone
func (c *CLRLogRecord) OriginalOp() LogRecordType {
	return c.originalOp
}

// Block returns the block ID for this record
func (c *CLRLogRecord) Block() *file.BlockID {
	return c.block
}

// Offset returns the offset for this record
func (c *CLRLogRecord) Offset() int {
	return c.offset
}

// GetValue returns the value to restore based on the original operation type
func (c *CLRLogRecord) GetIntValue() int {
	return c.intValue
}

func (c *CLRLogRecord) GetStringValue() string {
	return c.stringValue
}

func (c *CLRLogRecord) GetBoolValue() bool {
	return c.boolValue
}

// Undo for CLR records is a no-op in ARIES - CLRs are never undone
func (c *CLRLogRecord) Undo(tx *Transaction) error {
	// CLRs are never undone in ARIES
	return nil
}

// Redo performs the redo operation for this CLR record.
// This redoes the compensation (undo) operation.
func (c *CLRLogRecord) Redo(tx *Transaction) (bool, error) {
	pageLSN, err := tx.GetPageLSN(c.block)
	if err != nil {
		return false, err
	}
	if pageLSN >= c.lsn {
		return false, nil
	}

	// Redo the compensation operation based on the original operation type
	switch c.originalOp {
	case LogRecordSetInt:
		err = tx.SetInt(c.block, c.offset, c.intValue, false)
	case LogRecordSetString:
		err = tx.SetString(c.block, c.offset, c.stringValue, false)
	case LogRecordSetBool:
		err = tx.SetBool(c.block, c.offset, c.boolValue, false)
	default:
		// Should not happen
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// WriteCLRLogRecord writes a CLR log record to the log manager
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [undoNextLSN(8)] [originalOp(4)] [filename(4+len)] [blockNum(4)] [offset(4)] [value...] [crc32(4)]
func WriteCLRLogRecord(lm *log.Manager, txNum int64, lsn int64, prevLSN int64, undoNextLSN int64, originalOp LogRecordType, blk *file.BlockID, offset int, intVal int, stringVal string, boolVal bool) error {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	undoNextLSNPos := prevLSNPos + 8
	originalOpPos := undoNextLSNPos + 8
	fileNamePos := originalOpPos + 4
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	valuePos := offsetPos + 4

	var dataLen int
	switch originalOp {
	case LogRecordSetInt:
		dataLen = valuePos + 4
	case LogRecordSetString:
		dataLen = valuePos + 4 + len(stringVal)
	case LogRecordSetBool:
		dataLen = valuePos + 1
	default:
		dataLen = valuePos
	}
	finalLen := dataLen + CRC32ChecksumSize()

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordCLR))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)
	page.SetInt64Raw(undoNextLSNPos, undoNextLSN)
	page.SetIntRaw(originalOpPos, int(originalOp))
	page.SetStringRaw(fileNamePos, blk.Filename())
	page.SetIntRaw(blockNumPos, blk.Number())
	page.SetIntRaw(offsetPos, offset)

	// Write the value based on the original operation type
	switch originalOp {
	case LogRecordSetInt:
		page.SetIntRaw(valuePos, intVal)
	case LogRecordSetString:
		page.SetStringRaw(valuePos, stringVal)
	case LogRecordSetBool:
		page.SetBoolRaw(valuePos, boolVal)
	default:
		// No value to write for unknown operations
	}

	// Append CRC32 checksum
	appendCRC32(page, dataLen)

	return lm.Append(page.Bytes(), lsn)
}
