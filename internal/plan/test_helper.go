package plan

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/transaction"
)

// setupTestDB creates a test database environment for plan tests.
func setupTestDB(t *testing.T) (string, *transaction.Transaction, *metadata.Manager, func()) {
	tempDir, err := os.MkdirTemp("", "plan_test_*")
	require.NoError(t, err)

	dbPath := filepath.Join(tempDir, "testdb")

	fm, err := file.NewManager(dbPath, 400)
	require.NoError(t, err)
	lm, err := log.NewManager(fm, "testlog")
	require.NoError(t, err)
	bm, err := buffer.NewManager(fm, lm, 20)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := transaction.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()

	transactionManager := transaction.NewTransactionManager(fm, lm, bm, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	md := metadata.NewManager(true, tx)

	cleanup := func() {
		tx.Commit()
		os.RemoveAll(tempDir)
	}

	return dbPath, tx, md, cleanup
}
