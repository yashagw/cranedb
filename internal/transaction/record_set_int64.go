package transaction

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

type SetInt64LogRecord struct {
	LogRecord
	txNum    int64
	lsn      int64
	prevLSN  int64
	offset   int
	oldValue int64
	newValue int64
	block    *file.BlockID
}

// NewSetInt64LogRecord creates a new SetInt64LogRecord
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(8)] [newvalue(8)]
func NewSetInt64LogRecord(page *file.Page) *SetInt64LogRecord {
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
	oldValue := page.GetInt64Raw(oldValuePos)

	newValuePos := oldValuePos + 8
	newValue := page.GetInt64Raw(newValuePos)

	block := file.NewBlockID(fileName, blockNum)

	return &SetInt64LogRecord{
		txNum:    txNum,
		lsn:      lsn,
		prevLSN:  prevLSN,
		offset:   offset,
		oldValue: oldValue,
		newValue: newValue,
		block:    block,
	}
}

func (s *SetInt64LogRecord) Op() LogRecordType {
	return LogRecordSetInt64
}

func (s *SetInt64LogRecord) TxNumber() int64 {
	return s.txNum
}

func (s *SetInt64LogRecord) LSN() int64 {
	return s.lsn
}

func (s *SetInt64LogRecord) PrevLSN() int64 {
	return s.prevLSN
}

func (s *SetInt64LogRecord) Block() *file.BlockID {
	return s.block
}

func (s *SetInt64LogRecord) Offset() int {
	return s.offset
}

func (s *SetInt64LogRecord) OldValue() int64 {
	return s.oldValue
}

func (s *SetInt64LogRecord) NewValue() int64 {
	return s.newValue
}

func (s *SetInt64LogRecord) Undo(tx *Transaction) error {
	return tx.SetInt64(s.block, s.offset, s.oldValue, false)
}

func (s *SetInt64LogRecord) Redo(tx *Transaction) (bool, error) {
	pageLSN, err := tx.GetPageLSN(s.block)
	if err != nil {
		return false, err
	}
	if pageLSN >= s.lsn {
		return false, nil
	}

	err = tx.SetInt64(s.block, s.offset, s.newValue, false)
	if err != nil {
		return false, err
	}

	return true, nil
}

// WriteSetInt64LogRecord writes a SetInt64LogRecord to the log manager
// Page format: [op(4)] [txNum(8)] [lsn(8)] [prevLSN(8)] [filename(4+len(filename))] [blockNum(4)] [offset(4)] [oldvalue(8)] [newvalue(8)] [crc32(4)]
func WriteSetInt64LogRecord(lm *log.Manager, txNum int64, lsn int64, prevLSN int64, blk *file.BlockID, offset int, oldValue int64, newValue int64) error {
	opPos := 0
	txNumPos := opPos + LogRecordTypeSize()
	lsnPos := txNumPos + 8
	prevLSNPos := lsnPos + 8
	fileNamePos := prevLSNPos + 8
	blockNumPos := fileNamePos + 4 + len(blk.Filename())
	offsetPos := blockNumPos + 4
	oldValuePos := offsetPos + 4
	newValuePos := oldValuePos + 8
	dataLen := newValuePos + 8
	finalLen := dataLen + CRC32ChecksumSize()

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordSetInt64))
	page.SetInt64Raw(txNumPos, txNum)
	page.SetInt64Raw(lsnPos, lsn)
	page.SetInt64Raw(prevLSNPos, prevLSN)
	page.SetStringRaw(fileNamePos, blk.Filename())
	page.SetIntRaw(blockNumPos, blk.Number())
	page.SetIntRaw(offsetPos, offset)
	page.SetInt64Raw(oldValuePos, oldValue)
	page.SetInt64Raw(newValuePos, newValue)

	appendCRC32(page, dataLen)

	return lm.Append(page.Bytes(), lsn)
}
