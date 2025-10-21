package transaction

import (
	"sync"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
)

var (
	txNumMutex sync.Mutex
	nextTxNum  int
)

// getNextTxNum returns a unique transaction number using a global mutex
func getNextTxNum() int {
	txNumMutex.Lock()
	defer txNumMutex.Unlock()
	txNum := nextTxNum
	nextTxNum++
	return txNum
}

const (
	END_OF_LOG_RECORD = -1
)

type Transaction struct {
	fileManager        *file.Manager
	logManager         *log.Manager
	bufferManager      *buffer.Manager
	recoveryManager    *RecoveryManager
	concurrencyManager *ConcurrencyManager

	txNum      int
	bufferList *BufferList
}

// NewTransaction creates a new transaction
func NewTransaction(fileManager *file.Manager, logManager *log.Manager, bufferManager *buffer.Manager, lockTable *LockTable) *Transaction {
	txNum := getNextTxNum()

	concurrencyManager := NewConcurrencyManager(lockTable)
	bufferList := NewBufferList(bufferManager)

	transaction := &Transaction{
		fileManager:        fileManager,
		logManager:         logManager,
		bufferManager:      bufferManager,
		concurrencyManager: concurrencyManager,
		txNum:              txNum,
		bufferList:         bufferList,
	}
	recoveryManager := NewRecoveryManager(txNum, transaction, logManager, bufferManager)
	transaction.recoveryManager = recoveryManager

	return transaction
}

func (t *Transaction) Commit() {
	t.recoveryManager.Commit()
	t.concurrencyManager.release()
	t.bufferList.UnpinAll()
}

func (t *Transaction) Rollback() {
	t.recoveryManager.Rollback()
	t.concurrencyManager.release()
	t.bufferList.UnpinAll()
}

func (t *Transaction) Pin(blk *file.BlockID) *buffer.Buffer {
	return t.bufferList.Pin(blk)
}

func (t *Transaction) Unpin(blk *file.BlockID) {
	t.bufferList.Unpin(blk)
}

func (t *Transaction) GetInt(blk *file.BlockID, offset int) int {
	t.concurrencyManager.sLock(blk)
	buff := t.bufferList.GetBuffer(blk)
	val := buff.Contents().GetInt(offset)
	return val
}

func (t *Transaction) GetString(blk *file.BlockID, offset int) string {
	t.concurrencyManager.sLock(blk)
	buff := t.bufferList.GetBuffer(blk)
	val := buff.Contents().GetString(offset)
	return val
}

func (t *Transaction) SetInt(blk *file.BlockID, offset int, val int, log bool) {
	t.concurrencyManager.xLock(blk)
	buff := t.bufferList.GetBuffer(blk)
	lsn := -1
	if log {
		lsn = t.recoveryManager.SetInt(buff, offset)
	}
	page := buff.Contents()
	page.SetInt(offset, val)
	buff.SetModified(t.txNum, lsn)
}

func (t *Transaction) SetString(blk *file.BlockID, offset int, val string, log bool) {
	t.concurrencyManager.xLock(blk)
	buff := t.bufferList.GetBuffer(blk)
	lsn := -1
	if log {
		lsn = t.recoveryManager.SetString(buff, offset)
	}
	page := buff.Contents()
	page.SetString(offset, val)
	buff.SetModified(t.txNum, lsn)
}

func (t *Transaction) Size(filename string) (int, error) {
	dummyBlock := file.NewBlockID(filename, END_OF_LOG_RECORD)
	err := t.concurrencyManager.sLock(dummyBlock)
	if err != nil {
		return 0, err
	}
	return t.fileManager.GetNumBlocks(filename)
}

func (t *Transaction) Append(filename string) (*file.BlockID, error) {
	dummyBlock := file.NewBlockID(filename, END_OF_LOG_RECORD)
	err := t.concurrencyManager.xLock(dummyBlock)
	if err != nil {
		return nil, err
	}
	return t.fileManager.Append(filename), nil
}

func (t *Transaction) BlockSize() int {
	return t.fileManager.BlockSize()
}
