package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestRecordPage_Format(t *testing.T) {
	// Setup test environment
	fileManager := file.NewManager("/tmp/testdb", 400)
	logManager := log.NewManager(fileManager, "test.log")
	bufferManager := buffer.NewManager(fileManager, logManager, 10)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 20)

	layout := NewLayoutFromSchema(schema)
	require.NotNil(t, layout)

	block, err := tx.Append("testfile")
	require.NoError(t, err)
	require.NotNil(t, block)

	recordPage := NewRecordPage(tx, block, layout)
	require.NotNil(t, recordPage)

	// Test 1: Insert some data into slots first
	slot1 := recordPage.InsertSlot(-1)
	require.GreaterOrEqual(t, slot1, 0)
	recordPage.SetInt(slot1, "id", 42)
	recordPage.SetString(slot1, "name", "test")

	slot2 := recordPage.InsertSlot(slot1)
	require.Greater(t, slot2, slot1)
	recordPage.SetInt(slot2, "id", 100)
	recordPage.SetString(slot2, "name", "example")

	assert.Equal(t, SlotStatusInUse, recordPage.getSlotStatus(slot1))
	assert.Equal(t, SlotStatusInUse, recordPage.getSlotStatus(slot2))
	assert.Equal(t, 42, recordPage.GetInt(slot1, "id"))
	assert.Equal(t, "test", recordPage.GetString(slot1, "name"))
	assert.Equal(t, 100, recordPage.GetInt(slot2, "id"))
	assert.Equal(t, "example", recordPage.GetString(slot2, "name"))

	// Test 2: Format the record page
	recordPage.Format()

	assert.Equal(t, SlotStatusEmpty, recordPage.getSlotStatus(slot1))
	assert.Equal(t, SlotStatusEmpty, recordPage.getSlotStatus(slot2))
	assert.Equal(t, 0, recordPage.GetInt(slot1, "id"))
	assert.Equal(t, 0, recordPage.GetInt(slot2, "id"))
	assert.Equal(t, "", recordPage.GetString(slot1, "name"))
	assert.Equal(t, "", recordPage.GetString(slot2, "name"))

	// Test 4: Verify we can still insert new slots after formatting
	newSlot := recordPage.InsertSlot(-1)
	require.GreaterOrEqual(t, newSlot, 0)
	recordPage.SetInt(newSlot, "id", 999)
	recordPage.SetString(newSlot, "name", "newdata")
	assert.Equal(t, 999, recordPage.GetInt(newSlot, "id"))
	assert.Equal(t, "newdata", recordPage.GetString(newSlot, "name"))

	// Cleanup
	tx.Commit()
}
