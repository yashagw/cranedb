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

func TestMetadataManager_BasicOperations(t *testing.T) {
	dbDir := "testdata_metadata"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm, err := log.NewManager(fm, "testlog")
	assert.NoError(t, err)
	defer lm.Close()

	bm, err := buffer.NewManager(fm, lm, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	// Test 1: Create new MetadataManager (isNew = true)
	tx1 := transaction.NewTransaction(fm, lm, bm, lockTable)
	mm := NewManager(true, tx1)
	require.NotNil(t, mm)
	assert.NotNil(t, mm.tableManager)
	assert.NotNil(t, mm.viewManager)
	assert.NotNil(t, mm.indexManager)
	assert.NotNil(t, mm.statsManager)
	tx1.Commit()

	// Test 2: Create MetadataManager for existing database (isNew = false)
	tx2 := transaction.NewTransaction(fm, lm, bm, lockTable)
	mm2 := NewManager(false, tx2)
	require.NotNil(t, mm2)
	assert.NotNil(t, mm2.tableManager)
	assert.NotNil(t, mm2.viewManager)
	assert.NotNil(t, mm2.indexManager)
	assert.NotNil(t, mm2.statsManager)
	tx2.Commit()

	// Test 3: Create a table through MetadataManager
	tx3 := transaction.NewTransaction(fm, lm, bm, lockTable)
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 50)
	schema.AddStringField("email", 100)
	err = mm.CreateTable("users", schema, tx3)
	require.NoError(t, err, "Should create table successfully")
	tx3.Commit()

	// Test 4: Get table layout through MetadataManager
	tx4 := transaction.NewTransaction(fm, lm, bm, lockTable)
	layout, err := mm.GetTableLayout("users", tx4)
	require.NoError(t, err, "Should retrieve layout successfully")
	require.NotNil(t, layout)

	retrievedSchema := layout.GetSchema()
	assert.Equal(t, schema.Fields(), retrievedSchema.Fields(), "Retrieved schema should have same fields as original")
	assert.Equal(t, 3, len(retrievedSchema.Fields()), "Should have 3 fields")

	// Verify field types and lengths
	for _, fieldName := range schema.Fields() {
		assert.Equal(t, schema.Type(fieldName), retrievedSchema.Type(fieldName),
			"Field %s type should match", fieldName)
		assert.Equal(t, schema.Length(fieldName), retrievedSchema.Length(fieldName),
			"Field %s length should match", fieldName)
	}
	tx4.Commit()

	// Test 5: Create a view through MetadataManager
	tx5 := transaction.NewTransaction(fm, lm, bm, lockTable)
	viewDef := "SELECT id, name FROM users WHERE id > 0"
	err = mm.CreateView("user_view", viewDef, tx5)
	require.NoError(t, err, "Should create view successfully")
	tx5.Commit()

	// Test 6: Get view definition through MetadataManager
	tx6 := transaction.NewTransaction(fm, lm, bm, lockTable)
	retrievedViewDef, err := mm.GetViewDef("user_view", tx6)
	require.NoError(t, err, "Should retrieve view definition successfully")
	assert.Equal(t, viewDef, retrievedViewDef, "Retrieved view definition should match original")
	tx6.Commit()

	// Test 7: Create an index through MetadataManager
	tx7 := transaction.NewTransaction(fm, lm, bm, lockTable)
	err = mm.CreateIndex("users_id_idx", "users", "id", tx7)
	require.NoError(t, err, "Should create index successfully")
	tx7.Commit()

	// Test 8: Get index info through MetadataManager
	tx8 := transaction.NewTransaction(fm, lm, bm, lockTable)
	indexInfo, err := mm.GetIndexInfo("users", tx8)
	require.NoError(t, err, "Should get index info successfully")
	require.NotNil(t, indexInfo)
	assert.Equal(t, 1, len(indexInfo), "Should have 1 index")
	assert.Equal(t, "users_id_idx", indexInfo["id"].indexName)
	assert.Equal(t, "id", indexInfo["id"].fieldName)
	tx8.Commit()

	// Test 9: Get stat info through MetadataManager
	tx9 := transaction.NewTransaction(fm, lm, bm, lockTable)
	statInfo, err := mm.GetStatInfo("users", layout, tx9)
	require.NoError(t, err)
	require.NotNil(t, statInfo)
	assert.Equal(t, 0, statInfo.BlocksAccessed(), "Empty table should have 0 blocks accessed")
	assert.Equal(t, 0, statInfo.RecordsOutput(), "Empty table should have 0 records output")
	tx9.Commit()

	// Test 10: Try to get layout for non-existent table
	tx10 := transaction.NewTransaction(fm, lm, bm, lockTable)
	_, err = mm.GetTableLayout("nonexistent", tx10)
	require.Error(t, err, "Should return error for non-existent table")
	assert.Contains(t, err.Error(), "not found")
	tx10.Commit()

	// Test 11: Try to get view definition for non-existent view
	tx11 := transaction.NewTransaction(fm, lm, bm, lockTable)
	viewDef, err = mm.GetViewDef("nonexistent_view", tx11)
	require.NoError(t, err, "Should not return error for non-existent view")
	assert.Equal(t, "", viewDef, "Should return empty string for non-existent view")
	tx11.Commit()
}
