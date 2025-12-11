package transaction

import (
	"sync"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
)

var (
	txNumMutex sync.Mutex
	nextTxNum  int64
)

// getNextTxNum returns a unique transaction number using a global mutex
func getNextTxNum() int64 {
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
	logManager         *dblog.Manager
	bufferManager      *buffer.Manager
	recoveryManager    *RecoveryManager
	concurrencyManager *ConcurrencyManager
	transactionManager *TransactionManager

	dirtyPageTable *buffer.DirtyPageTable

	txNum      int64
	prevTxLSN  int64
	bufferList *BufferList
}

func (t *Transaction) Commit() error {
	err := t.recoveryManager.Commit()
	if err != nil {
		return err
	}
	err = t.concurrencyManager.release()
	if err != nil {
		return err
	}
	t.bufferList.UnpinAll()
	t.transactionManager.EndTransaction(t.txNum, TransactionStatusCommitted)

	return nil
}

func (t *Transaction) Rollback() error {
	err := t.recoveryManager.Rollback()
	if err != nil {
		return err
	}
	err = t.concurrencyManager.release()
	if err != nil {
		return err
	}
	t.bufferList.UnpinAll()
	t.transactionManager.EndTransaction(t.txNum, TransactionStatusAborted)

	return nil
}

func (t *Transaction) GetInt(blk *file.BlockID, offset int) (int, error) {
	err := t.concurrencyManager.sLock(blk)
	if err != nil {
		return 0, err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return 0, err
		}
	}

	val := buff.Contents().GetInt(offset)
	return val, nil
}

func (t *Transaction) GetBool(blk *file.BlockID, offset int) (bool, error) {
	err := t.concurrencyManager.sLock(blk)
	if err != nil {
		return false, err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return false, err
		}
	}

	val := buff.Contents().GetBool(offset)
	return val, nil
}

func (t *Transaction) GetString(blk *file.BlockID, offset int) (string, error) {
	err := t.concurrencyManager.sLock(blk)
	if err != nil {
		return "", err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return "", err
		}
	}

	val := buff.Contents().GetString(offset)
	return val, nil
}

// GetPageLSN reads the PageLSN from the page header with proper concurrency control.
// This is used during recovery to check if a page already has a log record applied.
func (t *Transaction) GetPageLSN(blk *file.BlockID) (int64, error) {
	err := t.concurrencyManager.sLock(blk)
	if err != nil {
		return -1, err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		var err error
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return -1, err
		}
	}

	val := buff.Contents().GetPageLSN()
	return val, nil
}

// ForceUpdatePageLSN is used exclusively during the ARIES Undo Pass
// after a CLR is written.
func (t *Transaction) ForceUpdatePageLSN(blk *file.BlockID, clrLSN int64) error {
	err := t.concurrencyManager.xLock(blk)
	if err != nil {
		return err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return err
		}
	}

	buff.SetModifiedTx(t.txNum)
	buff.SetModifiedLSN(clrLSN)

	// TODO: Do we need it here?
	buff.SetBufferPageLSN(clrLSN)

	return nil
}

func (t *Transaction) SetInt(blk *file.BlockID, offset int, val int, log bool) error {
	err := t.concurrencyManager.xLock(blk)
	if err != nil {
		return err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return err
		}
	}

	lsn := int64(-1)
	if log {
		lsn, err = t.recoveryManager.SetInt(buff, offset, val)
		if err != nil {
			return err
		}
		t.dirtyPageTable.Add(blk, lsn)
	}

	page := buff.Contents()
	page.SetInt(offset, val)
	buff.SetModifiedTx(t.txNum)
	buff.SetBufferPageLSN(lsn)

	return nil
}

func (t *Transaction) SetBool(blk *file.BlockID, offset int, val bool, log bool) error {
	err := t.concurrencyManager.xLock(blk)
	if err != nil {
		return err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return err
		}
	}

	lsn := int64(-1)
	if log {
		lsn, err = t.recoveryManager.SetBool(buff, offset, val)
		if err != nil {
			return err
		}
		t.dirtyPageTable.Add(blk, lsn)
	}

	page := buff.Contents()
	page.SetBool(offset, val)
	buff.SetModifiedTx(t.txNum)
	buff.SetModifiedLSN(lsn)

	return nil
}

func (t *Transaction) SetString(blk *file.BlockID, offset int, val string, log bool) error {
	err := t.concurrencyManager.xLock(blk)
	if err != nil {
		return err
	}

	buff := t.bufferList.GetBuffer(blk)
	if buff == nil {
		// Buffer not pinned yet, pin it first
		buff, err = t.bufferList.Pin(blk)
		if err != nil {
			return err
		}
	}

	lsn := int64(-1)
	if log {
		lsn, err = t.recoveryManager.SetString(buff, offset, val)
		if err != nil {
			return err
		}
		// TODO: maybe move it to buffer.SetModified?
		// Also dirtyPageTable is in the hotPath here
		// Each SetXXX calls dirtyPageTable.Add which acquires a global lock
		// Consider optimizing this later if it becomes a bottleneck
		t.dirtyPageTable.Add(blk, lsn)
	}

	page := buff.Contents()
	page.SetString(offset, val)
	buff.SetModifiedTx(t.txNum)
	buff.SetModifiedLSN(lsn)

	return nil
}

func (t *Transaction) Pin(blk *file.BlockID) (*buffer.Buffer, error) {
	return t.bufferList.Pin(blk)
}

func (t *Transaction) Unpin(blk *file.BlockID) {
	t.bufferList.Unpin(blk)
}

func (t *Transaction) Size(filename string) (int, error) {
	dummyBlock := file.NewBlockID(filename, END_OF_LOG_RECORD)
	err := t.concurrencyManager.sLock(dummyBlock)
	if err != nil {
		return 0, err
	}
	return t.fileManager.GetTotalBlocks(filename)
}

func (t *Transaction) Append(filename string) (*file.BlockID, error) {
	dummyBlock := file.NewBlockID(filename, END_OF_LOG_RECORD)
	err := t.concurrencyManager.xLock(dummyBlock)
	if err != nil {
		return nil, err
	}
	return t.fileManager.Append(filename)
}

func (t *Transaction) BlockSize() int {
	return t.fileManager.BlockSize()
}
