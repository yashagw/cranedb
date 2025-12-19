package transaction

import (
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

// TODO: Optimize it and break it into BEGIN and END checkpoint records
// So that a long TT and DPT can be written in multiple log records
// Also crash might happen while writing big checkpoint record
// so it is always good to have smaller records
type CheckpointLogRecord struct {
	LogRecord
	lsn              int64
	transactionTable map[int64]*TransactionEntry
	dirtyPageTable   map[file.BlockID]*buffer.DirtyPageEntry
}

// NewCheckpointLogRecord creates a new CheckpointLogRecord
// Page format: [op(4)] [lsn(8)] [txTableSize(4)] [txTableData...] [dptSize(4)] [dptData...]
// txTableData: for each tx: [txNum(8)] [status(4)] [lastLSN(8)]
// dptData: for each page: [filenameLen(4)] [filename] [blkNum(4)] [recLSN(8)]
func NewCheckpointLogRecord(page *file.Page) *CheckpointLogRecord {
	opPos := 0
	lsnPos := opPos + LogRecordTypeSize()
	lsn := page.GetInt64Raw(lsnPos)

	txTablePos := lsnPos + 8
	txTableSize := int(page.GetIntRaw(txTablePos))
	txTablePos += 4

	transactionTable := make(map[int64]*TransactionEntry, txTableSize)
	for i := 0; i < txTableSize; i++ {
		txNum := page.GetInt64Raw(txTablePos)
		txTablePos += 8
		status := TransactionStatus(page.GetIntRaw(txTablePos))
		txTablePos += 4
		lastLSN := page.GetInt64Raw(txTablePos)
		txTablePos += 8

		transactionTable[txNum] = &TransactionEntry{
			Status:  status,
			LastLSN: lastLSN,
		}
	}

	dptSizePos := txTablePos
	dptSize := int(page.GetIntRaw(dptSizePos))
	dptPos := dptSizePos + 4

	dirtyPageTable := make(map[file.BlockID]*buffer.DirtyPageEntry, dptSize)
	for i := 0; i < dptSize; i++ {
		filenameBytes := page.GetBytesArrayRaw(dptPos)
		filename := string(filenameBytes)
		dptPos += 4 + len(filenameBytes)

		// Read block number
		blkNum := page.GetIntRaw(dptPos)
		dptPos += 4

		// Read recLSN
		recLSN := page.GetInt64Raw(dptPos)
		dptPos += 8

		blockID := file.NewBlockID(filename, blkNum)
		key := file.MakeBlockKey(blockID)
		dirtyPageTable[key] = &buffer.DirtyPageEntry{
			RecLSN: recLSN,
		}
	}

	return &CheckpointLogRecord{
		lsn:              lsn,
		transactionTable: transactionTable,
		dirtyPageTable:   dirtyPageTable,
	}
}

// Op returns the operation type for this log record
func (s *CheckpointLogRecord) Op() LogRecordType {
	return LogRecordCheckpoint
}

// TxNumber returns the transaction number associated with this log record
func (s *CheckpointLogRecord) TxNumber() int64 {
	// Checkpoint record is not associated with any transaction
	return -1
}

// LSN returns the log sequence number for this record
func (s *CheckpointLogRecord) LSN() int64 {
	return s.lsn
}

// TransactionTable returns the transaction table snapshot from the checkpoint
func (s *CheckpointLogRecord) TransactionTable() map[int64]*TransactionEntry {
	return s.transactionTable
}

// DirtyPageTable returns the dirty page table snapshot from the checkpoint
func (s *CheckpointLogRecord) DirtyPageTable() map[file.BlockID]*buffer.DirtyPageEntry {
	return s.dirtyPageTable
}

// Undo performs the undo operation for this log record
func (s *CheckpointLogRecord) Undo(tx *Transaction) error {
	// No need to undo anything for Checkpoint Record
	return nil
}

// Redo performs the redo operation for this log record.
// Checkpoint records don't need redo (they're metadata).
func (s *CheckpointLogRecord) Redo(tx *Transaction) (bool, error) {
	return false, nil
}

// WriteCheckpointLogRecord writes a CheckpointLogRecord to the log manager
// Page format: [op(4)] [lsn(8)] [txTableSize(4)] [txTableData...] [dptSize(4)] [dptData...] [crc32(4)]
func WriteCheckpointLogRecord(lm *log.Manager, lsn int64, transactionTable map[int64]*TransactionEntry, dirtyPageTable map[file.BlockID]*buffer.DirtyPageEntry) error {
	opPos := 0
	lsnPos := opPos + LogRecordTypeSize()
	txTableSizePos := lsnPos + 8
	txTableDataPos := txTableSizePos + 4

	// Calculate size needed for transaction table
	txTableDataSize := 0
	for range transactionTable {
		txTableDataSize += 8 + 4 + 8 // txNum(8) + status(4) + lastLSN(8)
	}

	// Calculate size needed for dirty page table
	dptSizePos := txTableDataPos + txTableDataSize
	dptDataPos := dptSizePos + 4
	dptDataSize := 0
	for blockID := range dirtyPageTable {
		dptDataSize += 4 + len(blockID.Filename()) + 4 + 8
	}

	dataLen := dptDataPos + dptDataSize
	finalLen := dataLen + CRC32ChecksumSize()

	page := file.NewPage(finalLen)
	page.SetIntRaw(opPos, int(LogRecordCheckpoint))
	page.SetInt64Raw(lsnPos, lsn)

	// Write transaction table
	page.SetIntRaw(txTableSizePos, len(transactionTable))
	pos := txTableDataPos
	for txNum, entry := range transactionTable {
		page.SetInt64Raw(pos, txNum)
		pos += 8
		page.SetIntRaw(pos, int(entry.Status))
		pos += 4
		page.SetInt64Raw(pos, entry.LastLSN)
		pos += 8
	}

	// Write dirty page table
	page.SetIntRaw(dptSizePos, len(dirtyPageTable))
	pos = dptDataPos
	for blockID, entry := range dirtyPageTable {
		page.SetBytesArrayRaw(pos, []byte(blockID.Filename()))
		pos += 4 + len(blockID.Filename())

		// Write block number
		page.SetIntRaw(pos, blockID.Number())
		pos += 4

		// Write recLSN
		page.SetInt64Raw(pos, entry.RecLSN)
		pos += 8
	}

	// Append CRC32 checksum
	appendCRC32(page, dataLen)

	return lm.Append(page.Bytes(), lsn)
}
