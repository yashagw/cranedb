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
	fileManager, err := file.NewManager("/tmp/testdb", 400)
	assert.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	assert.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	assert.NoError(t, err)
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

	recordPage, err := NewRecordPage(tx, block, layout)
	require.NoError(t, err)
	require.NotNil(t, recordPage)

	// Test 1: Insert some data into slots first
	slot1, err := recordPage.InsertSlot(-1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, slot1, 0)
	err = recordPage.SetInt(slot1, "id", 42)
	require.NoError(t, err)
	err = recordPage.SetString(slot1, "name", "test")
	require.NoError(t, err)

	slot2, err := recordPage.InsertSlot(slot1)
	require.NoError(t, err)
	require.Greater(t, slot2, slot1)
	err = recordPage.SetInt(slot2, "id", 100)
	require.NoError(t, err)
	err = recordPage.SetString(slot2, "name", "example")
	require.NoError(t, err)

	status1, err := recordPage.getSlotStatus(slot1)
	require.NoError(t, err)
	assert.Equal(t, SlotStatusInUse, status1)
	status2, err := recordPage.getSlotStatus(slot2)
	require.NoError(t, err)
	assert.Equal(t, SlotStatusInUse, status2)
	id1, err := recordPage.GetInt(slot1, "id")
	require.NoError(t, err)
	assert.Equal(t, 42, id1)
	name1, err := recordPage.GetString(slot1, "name")
	require.NoError(t, err)
	assert.Equal(t, "test", name1)
	id2, err := recordPage.GetInt(slot2, "id")
	require.NoError(t, err)
	assert.Equal(t, 100, id2)
	name2, err := recordPage.GetString(slot2, "name")
	require.NoError(t, err)
	assert.Equal(t, "example", name2)

	// Test 2: Format the record page
	recordPage.Format()

	status1, err = recordPage.getSlotStatus(slot1)
	require.NoError(t, err)
	assert.Equal(t, SlotStatusEmpty, status1)
	status2, err = recordPage.getSlotStatus(slot2)
	require.NoError(t, err)
	assert.Equal(t, SlotStatusEmpty, status2)
	id1, err = recordPage.GetInt(slot1, "id")
	require.NoError(t, err)
	assert.Equal(t, 0, id1)
	id2, err = recordPage.GetInt(slot2, "id")
	require.NoError(t, err)
	assert.Equal(t, 0, id2)
	name1, err = recordPage.GetString(slot1, "name")
	require.NoError(t, err)
	assert.Equal(t, "", name1)
	name2, err = recordPage.GetString(slot2, "name")
	require.NoError(t, err)
	assert.Equal(t, "", name2)

	// Test 4: Verify we can still insert new slots after formatting
	newSlot, err := recordPage.InsertSlot(-1)
	require.NoError(t, err)
	require.GreaterOrEqual(t, newSlot, 0)
	err = recordPage.SetInt(newSlot, "id", 999)
	require.NoError(t, err)
	err = recordPage.SetString(newSlot, "name", "newdata")
	require.NoError(t, err)
	newID, err := recordPage.GetInt(newSlot, "id")
	require.NoError(t, err)
	assert.Equal(t, 999, newID)
	newName, err := recordPage.GetString(newSlot, "name")
	require.NoError(t, err)
	assert.Equal(t, "newdata", newName)

	// Cleanup
	tx.Commit()
}
