package metadata

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestIndexManager_BasicOperations(t *testing.T) {
	dbDir := "testdata"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm := log.NewManager(fm, "testlog")
	defer lm.Close()

	bm := buffer.NewManager(fm, lm, 10)
	lockTable := transaction.NewLockTable()

	// Test 1: Create new IndexManager with new database
	tx1 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm := NewTableManager(true, tx1)
	sm := NewStatsManager(tm, tx1)
	im := NewIndexManager(true, tm, sm, tx1)
	require.NotNil(t, im)
	tx1.Commit()

	// Test 2: Create a new table
	tx2 := transaction.NewTransaction(fm, lm, bm, lockTable)
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err = tm.CreateTable("users", schema, tx2)
	require.NoError(t, err)
	tx2.Commit()

	// Test 3: Create an index
	tx3 := transaction.NewTransaction(fm, lm, bm, lockTable)
	err = im.CreateIndex("users_id_idx", "users", "id", tx3)
	require.NoError(t, err, "Should create index successfully")
	tx3.Commit()

	// Test 4: Get index info
	tx4 := transaction.NewTransaction(fm, lm, bm, lockTable)
	indexInfo, err := im.GetIndexInfo("users", tx4)
	require.NoError(t, err, "Should get index info successfully")
	require.NotNil(t, indexInfo)
	tx4.Commit()

	assert.Equal(t, 1, len(indexInfo))
	assert.Equal(t, "users_id_idx", indexInfo["id"].indexName)
	assert.Equal(t, "id", indexInfo["id"].fieldName)
	assert.NotNil(t, indexInfo["id"].tableSchema)
	assert.NotNil(t, indexInfo["id"].indexLayout)
}
