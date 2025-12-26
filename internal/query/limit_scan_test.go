package query

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// setupLimitScanTestDB creates a test database with sample data
func setupLimitScanTestDB(t *testing.T, testDir string) (*transaction.Transaction, *table.TableScan) {
	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	require.NotNil(t, tx)

	// Create schema
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	layout := record.NewLayoutFromSchema(schema)
	require.NotNil(t, layout)

	// Create TableScan and insert test data
	ts, err := table.NewTableScan(tx, layout, "TestLimitTable")
	require.NoError(t, err)
	require.NotNil(t, ts)

	// Insert 10 records
	err = ts.BeforeFirst()
	require.NoError(t, err)
	for i := 1; i <= 10; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i)
		require.NoError(t, err)
		err = ts.SetString("name", "User"+string(rune('A'+i-1))) // UserA, UserB, ...
		require.NoError(t, err)
	}

	return tx, ts
}

func TestLimitScan(t *testing.T) {
	testDir := "/tmp/testdb_limitscan"
	defer os.RemoveAll(testDir)

	tx, ts := setupLimitScanTestDB(t, testDir)
	defer tx.Commit()

	t.Run("LimitOnly", func(t *testing.T) {
		ts.BeforeFirst()

		limit := 3
		limitScan := NewLimitScan(ts, limit, 0)
		require.NotNil(t, limitScan)

		err := limitScan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		var ids []int
		for {
			hasNext, err := limitScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := limitScan.GetInt("id")
			require.NoError(t, err)
			ids = append(ids, id)
			count++
		}

		assert.Equal(t, limit, count)
		assert.Equal(t, []int{1, 2, 3}, ids)
		limitScan.Close()
	})

	t.Run("OffsetOnly", func(t *testing.T) {
		ts.BeforeFirst()

		offset := 5
		limitScan := NewLimitScan(ts, 0, offset)
		require.NotNil(t, limitScan)

		err := limitScan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		var ids []int
		for {
			hasNext, err := limitScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := limitScan.GetInt("id")
			require.NoError(t, err)
			ids = append(ids, id)
			count++
		}

		// Should have 10 - 5 = 5 records
		assert.Equal(t, 5, count)
		assert.Equal(t, []int{6, 7, 8, 9, 10}, ids)
		limitScan.Close()
	})

	t.Run("LimitAndOffset", func(t *testing.T) {
		ts.BeforeFirst()

		limit := 3
		offset := 2
		limitScan := NewLimitScan(ts, limit, offset)
		require.NotNil(t, limitScan)

		err := limitScan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		var ids []int
		for {
			hasNext, err := limitScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			id, err := limitScan.GetInt("id")
			require.NoError(t, err)
			ids = append(ids, id)
			count++
		}

		// offset 2 skips 1, 2. Remaining starts at 3.
		// limit 3 takes 3, 4, 5.
		assert.Equal(t, limit, count)
		assert.Equal(t, []int{3, 4, 5}, ids)
		limitScan.Close()
	})

	t.Run("OffsetLargerThanInput", func(t *testing.T) {
		ts.BeforeFirst()

		offset := 20
		limitScan := NewLimitScan(ts, 0, offset)
		require.NotNil(t, limitScan)

		err := limitScan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := limitScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 0, count)
		limitScan.Close()
	})

	t.Run("LimitLargerThanInput", func(t *testing.T) {
		ts.BeforeFirst()

		limit := 20
		limitScan := NewLimitScan(ts, limit, 0)
		require.NotNil(t, limitScan)

		err := limitScan.BeforeFirst()
		require.NoError(t, err)

		count := 0
		for {
			hasNext, err := limitScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 10, count) // Only 10 records available
		limitScan.Close()
	})
}
