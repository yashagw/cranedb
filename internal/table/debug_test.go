package table

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestDebugVisibility(t *testing.T) {
	testDir := "/tmp/testdb_debug_vis"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 4096)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	tm := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := tm.BeginTransaction()
	require.NotNil(t, tx)
	
	fmt.Printf("Transaction number: %d\n", tx.TxNum())
	fmt.Printf("Snapshot: %+v\n", tx.GetSnapshot())
	fmt.Printf("CommitLog: %+v\n", tx.GetCommitLog())

	schema := record.NewSchema()
	schema.AddIntField("A")
	schema.AddStringField("B", 9)
	schema.AddBoolField("C")
	layout := record.NewLayoutFromSchema(schema)
	fmt.Printf("Slot size: %d\n", layout.GetSlotSize())

	ts, err := NewTableScan(tx, layout, "DebugTable")
	require.NoError(t, err)
	
	// Insert 3 records
	for i := 0; i < 3; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("A", i*10)
		require.NoError(t, err)
		err = ts.SetString("B", "rec")
		require.NoError(t, err)
		err = ts.SetBool("C", true)
		require.NoError(t, err)
		rid, _ := ts.GetRID()
		fmt.Printf("Inserted at block %d, slot %d\n", rid.Block(), rid.Slot())
	}
	
	// Check xmin/xmax directly
	err = ts.BeforeFirst()
	require.NoError(t, err)
	
	// Manually check slot 0
	xmin, err := ts.currentRecordPage.GetXmin(0)
	require.NoError(t, err)
	xmax, err := ts.currentRecordPage.GetXmax(0)
	require.NoError(t, err)
	fmt.Printf("Slot 0: xmin=%d, xmax=%d, txNum=%d\n", xmin, xmax, tx.TxNum())
	
	// Move to first record before checking visibility
	ts.currentSlot = 0
	visible, err := ts.isCurrentSlotVisible()
	fmt.Printf("isCurrentSlotVisible (slot=%d): visible=%v, err=%v\n", ts.currentSlot, visible, err)

	// Try Next() from beginning
	err = ts.BeforeFirst()
	require.NoError(t, err)
	hasNext, err := ts.Next()
	fmt.Printf("Next(): hasNext=%v, err=%v, currentSlot=%d\n", hasNext, err, ts.currentSlot)
	if hasNext {
		a, _ := ts.GetInt("A")
		fmt.Printf("A=%d\n", a)
	}
	
	ts.Close()
	tx.Commit()
}
