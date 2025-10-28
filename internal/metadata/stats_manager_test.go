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

func TestStatsManager_BasicOperations(t *testing.T) {
	dbDir := "testdata_stats"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm := log.NewManager(fm, "testlog")
	defer lm.Close()

	bm := buffer.NewManager(fm, lm, 10)
	lockTable := transaction.NewLockTable()

	// Test 1: Create new StatsManager
	tx1 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm := NewTableManager(true, tx1)
	require.NotNil(t, tm)
	tx1.Commit()

	tx2 := transaction.NewTransaction(fm, lm, bm, lockTable)
	sm := NewStatsManager(tm, tx2)
	require.NotNil(t, sm)
	assert.NotNil(t, sm.tblMgr)
	assert.NotNil(t, sm.tableStats)
	assert.Equal(t, 0, sm.numCalls)
	tx2.Commit()

	// Test 2: Create StatInfo with basic data
	tx3 := transaction.NewTransaction(fm, lm, bm, lockTable)
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	layout := record.NewLayoutFromSchema(schema)

	si := NewStatInfo(5, 100, layout)
	require.NotNil(t, si)
	assert.Equal(t, 5, si.BlocksAccessed())
	assert.Equal(t, 100, si.RecordsOutput())

	tx3.Commit()

	// Test 3: Get stats for non-existent table
	tx4 := transaction.NewTransaction(fm, lm, bm, lockTable)
	sm2 := NewStatsManager(tm, tx4)
	si2 := sm2.GetStatInfo("nonexistent", layout, tx4)
	require.NotNil(t, si2)
	assert.Equal(t, 0, si2.BlocksAccessed())
	assert.Equal(t, 0, si2.RecordsOutput())
	tx4.Commit()

	// Test 4: Create a table and get stats for empty table
	tx5 := transaction.NewTransaction(fm, lm, bm, lockTable)
	err = tm.CreateTable("test_table", schema, tx5)
	require.NoError(t, err, "Should create table successfully")
	tx5.Commit()

	tx6 := transaction.NewTransaction(fm, lm, bm, lockTable)
	layout2, err := tm.GetLayout("test_table", tx6)
	require.NoError(t, err, "Should retrieve layout successfully")
	require.NotNil(t, layout2)

	sm3 := NewStatsManager(tm, tx6)
	si3 := sm3.GetStatInfo("test_table", layout2, tx6)
	require.NotNil(t, si3)
	assert.Equal(t, 0, si3.BlocksAccessed())
	assert.Equal(t, 0, si3.RecordsOutput())
	tx6.Commit()

	// Test 5: Test refresh statistics
	tx7 := transaction.NewTransaction(fm, lm, bm, lockTable)
	schema3 := record.NewSchema()
	schema3.AddIntField("id")
	err = tm.CreateTable("refresh_test", schema3, tx7)
	require.NoError(t, err, "Should create refresh test table successfully")
	tx7.Commit()

	tx8 := transaction.NewTransaction(fm, lm, bm, lockTable)
	sm4 := NewStatsManager(tm, tx8)
	sm4.refreshStatistics(tx8)
	assert.NotNil(t, sm4.tableStats, "Table stats should not be nil after refresh")
	tx8.Commit()
}

func TestStatsManager_DistinctValues(t *testing.T) {
	dbDir := "testdata_distinct"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm := log.NewManager(fm, "testlog")
	defer lm.Close()

	bm := buffer.NewManager(fm, lm, 10)
	lockTable := transaction.NewLockTable()

	// Setup database
	tx1 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm := NewTableManager(true, tx1)
	require.NotNil(t, tm)
	tx1.Commit()

	// Create table with data
	tx2 := transaction.NewTransaction(fm, lm, bm, lockTable)
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)
	err = tm.CreateTable("test_table", schema, tx2)
	require.NoError(t, err, "Should create table successfully")
	tx2.Commit()

	// Insert data
	tx3 := transaction.NewTransaction(fm, lm, bm, lockTable)
	layout, err := tm.GetLayout("test_table", tx3)
	require.NoError(t, err, "Should retrieve layout successfully")
	require.NotNil(t, layout)

	ts := record.NewTableScan(tx3, layout, "test_table")
	defer ts.Close()

	testData := []struct {
		id   int
		name string
	}{
		{1, "alice"},
		{2, "bob"},
		{3, "alice"},
		{4, "charlie"},
		{5, "bob"},
	}

	for _, data := range testData {
		ts.Insert()
		ts.SetInt("id", data.id)
		ts.SetString("name", data.name)
	}
	tx3.Commit()

	// Test StatsManager with data
	tx4 := transaction.NewTransaction(fm, lm, bm, lockTable)
	sm := NewStatsManager(tm, tx4)
	si := sm.GetStatInfo("test_table", layout, tx4)
	require.NotNil(t, si)
	assert.Equal(t, 5, si.RecordsOutput())
	assert.Greater(t, si.BlocksAccessed(), 0)

	// Test distinct values calculation
	distinctIds := sm.GetDistinctValues("test_table", "id", layout, tx4)
	assert.Equal(t, 5, distinctIds, "Should have 5 distinct IDs")

	distinctNames := sm.GetDistinctValues("test_table", "name", layout, tx4)
	assert.Equal(t, 3, distinctNames, "Should have 3 distinct names")

	// Test caching
	distinctIds2 := sm.GetDistinctValues("test_table", "id", layout, tx4)
	assert.Equal(t, distinctIds, distinctIds2, "Cached result should match")
	tx4.Commit()
}
