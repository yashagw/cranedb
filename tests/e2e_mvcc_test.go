package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/mvcc"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestMVCC_OwnInsertsVisible(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_own_inserts")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	// Create table
	err := db.ExecuteUpdate("create table items (id int, name varchar(10))")
	require.NoError(t, err)

	// Begin a transaction, insert, and read within the same transaction
	tx := db.transactionManager.BeginTransaction()

	_, err = db.planner.ExecuteUpdate("insert into items (id, name) values (1, 'apple')", tx)
	require.NoError(t, err)
	_, err = db.planner.ExecuteUpdate("insert into items (id, name) values (2, 'banana')", tx)
	require.NoError(t, err)

	// Should see own inserts before commit
	plan, err := db.planner.CreatePlan("select id, name from items", tx, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	count := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}
	scan.Close()
	assert.Equal(t, 2, count, "Transaction should see its own uncommitted inserts")

	err = tx.Commit()
	require.NoError(t, err)
}

func TestMVCC_UncommittedInsertsInvisibleToOthers(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_uncommitted_invisible")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table products (id int, price int)")
	require.NoError(t, err)

	// tx1 inserts but does not commit
	tx1 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("insert into products (id, price) values (1, 100)", tx1)
	require.NoError(t, err)

	// tx2 should NOT see tx1's uncommitted insert
	tx2 := db.transactionManager.BeginTransaction()
	plan, err := db.planner.CreatePlan("select id from products", tx2, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	count := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}
	scan.Close()
	assert.Equal(t, 0, count, "Uncommitted inserts should be invisible to other transactions")

	err = tx2.Commit()
	require.NoError(t, err)
	err = tx1.Commit()
	require.NoError(t, err)
}

func TestMVCC_CommittedInsertsVisibleToNewTransactions(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_committed_visible")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table orders (id int, amount int)")
	require.NoError(t, err)

	// tx1 inserts and commits
	tx1 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("insert into orders (id, amount) values (1, 500)", tx1)
	require.NoError(t, err)
	err = tx1.Commit()
	require.NoError(t, err)

	// tx2 (started after tx1 commits) should see the insert
	tx2 := db.transactionManager.BeginTransaction()
	plan, err := db.planner.CreatePlan("select id, amount from orders", tx2, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	hasNext, err := scan.Next()
	require.NoError(t, err)
	require.True(t, hasNext, "Should see committed insert")

	id, err := scan.GetInt("id")
	require.NoError(t, err)
	assert.Equal(t, 1, id)

	amount, err := scan.GetInt("amount")
	require.NoError(t, err)
	assert.Equal(t, 500, amount)

	scan.Close()
	err = tx2.Commit()
	require.NoError(t, err)
}

func TestMVCC_SnapshotIsolation(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_snapshot_isolation")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table accounts (id int, balance int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into accounts (id, balance) values (1, 1000)")
	require.NoError(t, err)

	// tx1 starts reading (takes snapshot)
	tx1 := db.transactionManager.BeginTransaction()

	// tx2 updates the balance and commits
	tx2 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("update accounts set balance = 2000 where id = 1", tx2)
	require.NoError(t, err)
	err = tx2.Commit()
	require.NoError(t, err)

	// tx1 should still see the OLD value (snapshot isolation)
	plan, err := db.planner.CreatePlan("select balance from accounts where id = 1", tx1, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	hasNext, err := scan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	balance, err := scan.GetInt("balance")
	require.NoError(t, err)
	assert.Equal(t, 1000, balance, "Snapshot isolation: tx1 should see the value at snapshot time")

	scan.Close()
	err = tx1.Commit()
	require.NoError(t, err)

	// tx3 (new transaction) should see updated value
	tx3 := db.transactionManager.BeginTransaction()
	plan, err = db.planner.CreatePlan("select balance from accounts where id = 1", tx3, nil)
	require.NoError(t, err)
	scan, err = plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	hasNext, err = scan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	balance, err = scan.GetInt("balance")
	require.NoError(t, err)
	assert.Equal(t, 2000, balance, "New transaction should see committed update")

	scan.Close()
	err = tx3.Commit()
	require.NoError(t, err)
}

func TestMVCC_WriteConflict(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_write_conflict")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table counters (id int, val int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into counters (id, val) values (1, 0)")
	require.NoError(t, err)

	// tx1 updates the row (sets xmax on old version)
	tx1 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("update counters set val = 10 where id = 1", tx1)
	require.NoError(t, err)

	// tx2 tries to update the same row — should get write conflict
	tx2 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("update counters set val = 20 where id = 1", tx2)
	require.Error(t, err, "Second writer should get a conflict error")
	assert.Equal(t, transaction.ErrWriteConflict, err)

	err = tx2.Rollback()
	require.NoError(t, err)
	err = tx1.Commit()
	require.NoError(t, err)

	// Verify tx1's value won
	rows, err := db.ExecuteQuery("select val from counters where id = 1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 10, rows[0]["val"])
}

func TestMVCC_DeleteSetsXmax(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_delete_xmax")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table temp (id int, data varchar(5))")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into temp (id, data) values (1, 'a')")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into temp (id, data) values (2, 'b')")
	require.NoError(t, err)

	// tx1 starts (takes snapshot of both rows)
	tx1 := db.transactionManager.BeginTransaction()

	// tx2 deletes row 1 and commits
	tx2 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("delete from temp where id = 1", tx2)
	require.NoError(t, err)
	err = tx2.Commit()
	require.NoError(t, err)

	// tx1 should still see both rows (snapshot isolation)
	plan, err := db.planner.CreatePlan("select id from temp", tx1, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	count := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		count++
	}
	scan.Close()
	assert.Equal(t, 2, count, "tx1 should still see deleted row (snapshot isolation)")
	err = tx1.Commit()
	require.NoError(t, err)

	// New transaction should only see 1 row
	rows, err := db.ExecuteQuery("select id from temp")
	require.NoError(t, err)
	assert.Len(t, rows, 1, "New transaction should not see deleted row")
	assert.Equal(t, 2, rows[0]["id"])
}

func TestMVCC_DeleteConflict(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_delete_conflict")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table items2 (id int, name varchar(10))")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into items2 (id, name) values (1, 'x')")
	require.NoError(t, err)

	// tx1 deletes the row
	tx1 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("delete from items2 where id = 1", tx1)
	require.NoError(t, err)

	// tx2 tries to delete same row — should get write conflict
	tx2 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("delete from items2 where id = 1", tx2)
	require.Error(t, err)
	assert.Equal(t, transaction.ErrWriteConflict, err)

	err = tx2.Rollback()
	require.NoError(t, err)
	err = tx1.Commit()
	require.NoError(t, err)
}

func TestMVCC_RollbackRestoresVisibility(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_rollback")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table data (id int, val int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into data (id, val) values (1, 100)")
	require.NoError(t, err)

	// tx1 inserts a row then rolls back
	tx1 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("insert into data (id, val) values (2, 200)", tx1)
	require.NoError(t, err)
	err = tx1.Rollback()
	require.NoError(t, err)

	// New transaction should only see the original row
	rows, err := db.ExecuteQuery("select id, val from data")
	require.NoError(t, err)
	assert.Len(t, rows, 1, "Rolled-back insert should not be visible")
	assert.Equal(t, 1, rows[0]["id"])
	assert.Equal(t, 100, rows[0]["val"])
}

func TestMVCC_Vacuum(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_vacuum")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table vactest (id int, name varchar(10))")
	require.NoError(t, err)

	// Insert records
	for i := 1; i <= 5; i++ {
		err = db.ExecuteUpdate("insert into vactest (id, name) values (" + itoa(i) + ", 'row')")
		require.NoError(t, err)
	}

	// Delete some records
	err = db.ExecuteUpdate("delete from vactest where id = 2")
	require.NoError(t, err)
	err = db.ExecuteUpdate("delete from vactest where id = 4")
	require.NoError(t, err)

	// Verify 3 rows visible
	rows, err := db.ExecuteQuery("select id from vactest")
	require.NoError(t, err)
	assert.Len(t, rows, 3)

	// Run vacuum — should reclaim 2 dead slots
	tx := db.transactionManager.BeginTransaction()
	layout, err := db.metadataManager.GetTableLayout("vactest", tx)
	require.NoError(t, err)
	indexInfo, err := db.metadataManager.GetIndexInfo("vactest", tx)
	require.NoError(t, err)
	commitLog := db.transactionManager.GetCommitLog()
	oldestActive := db.transactionManager.GetOldestActiveTx()

	reclaimed, err := mvcc.Vacuum("vactest", layout, tx, commitLog, indexInfo, oldestActive)
	require.NoError(t, err)
	assert.Equal(t, 2, reclaimed, "Should reclaim 2 dead tuples")

	err = tx.Commit()
	require.NoError(t, err)

	// Verify still 3 rows visible after vacuum
	rows, err = db.ExecuteQuery("select id from vactest")
	require.NoError(t, err)
	assert.Len(t, rows, 3)
}

func TestMVCC_XminXmaxSetCorrectly(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_xmin_xmax")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table meta (id int)")
	require.NoError(t, err)

	// Insert a record and check xmin/xmax directly
	tx := db.transactionManager.BeginTransaction()
	txNum := tx.TxNum()

	layout, err := db.metadataManager.GetTableLayout("meta", tx)
	require.NoError(t, err)

	ts, err := table.NewTableScan(tx, layout, "meta")
	require.NoError(t, err)

	err = ts.Insert()
	require.NoError(t, err)
	err = ts.SetInt("id", 42)
	require.NoError(t, err)

	// Check xmin is set to current tx
	xmin, err := ts.GetXmin()
	require.NoError(t, err)
	assert.Equal(t, txNum, xmin, "xmin should be set to inserting transaction's ID")

	// Check xmax is 0 (not deleted)
	xmax, err := ts.GetXmax()
	require.NoError(t, err)
	assert.Equal(t, int64(0), xmax, "xmax should be 0 for live tuples")

	ts.Close()
	err = tx.Commit()
	require.NoError(t, err)

	// Delete the record and check xmax
	tx2 := db.transactionManager.BeginTransaction()
	tx2Num := tx2.TxNum()

	ts2, err := table.NewTableScan(tx2, layout, "meta")
	require.NoError(t, err)

	hasNext, err := ts2.Next()
	require.NoError(t, err)
	require.True(t, hasNext)

	err = ts2.Delete()
	require.NoError(t, err)

	// xmax should now be tx2's ID
	xmax, err = ts2.GetXmax()
	require.NoError(t, err)
	assert.Equal(t, tx2Num, xmax, "xmax should be set to deleting transaction's ID")

	ts2.Close()
	err = tx2.Commit()
	require.NoError(t, err)
}

func TestMVCC_UpdateCreatesNewVersion(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_update_version")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table versions (id int, val int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into versions (id, val) values (1, 10)")
	require.NoError(t, err)

	// Update creates a new version (old version gets xmax set, new version gets xmin)
	err = db.ExecuteUpdate("update versions set val = 20 where id = 1")
	require.NoError(t, err)

	// Should see the new value
	rows, err := db.ExecuteQuery("select val from versions where id = 1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 20, rows[0]["val"])

	// Check raw scan: should have 2 physical slots (old dead + new live)
	tx := db.transactionManager.BeginTransaction()
	layout, err := db.metadataManager.GetTableLayout("versions", tx)
	require.NoError(t, err)

	ts, err := table.NewTableScan(tx, layout, "versions")
	require.NoError(t, err)

	rawCount := 0
	for {
		hasNext, err := ts.NextRaw()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rawCount++
	}
	ts.Close()
	assert.Equal(t, 2, rawCount, "UPDATE should leave old version + create new version (2 physical slots)")
	err = tx.Commit()
	require.NoError(t, err)
}

func TestMVCC_ReaderDoesNotBlockWriter(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_no_reader_block")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table noblock (id int, val int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into noblock (id, val) values (1, 100)")
	require.NoError(t, err)

	// tx1 reads (reader)
	tx1 := db.transactionManager.BeginTransaction()
	plan, err := db.planner.CreatePlan("select val from noblock", tx1, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)
	hasNext, err := scan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)
	val, err := scan.GetInt("val")
	require.NoError(t, err)
	assert.Equal(t, 100, val)
	scan.Close()

	// tx2 writes while tx1 still holds its read — should NOT block
	tx2 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("update noblock set val = 200 where id = 1", tx2)
	require.NoError(t, err, "Writer should not be blocked by reader")
	err = tx2.Commit()
	require.NoError(t, err)

	// tx1 re-reads — should still see old value (snapshot)
	plan, err = db.planner.CreatePlan("select val from noblock", tx1, nil)
	require.NoError(t, err)
	scan, err = plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)
	hasNext, err = scan.Next()
	require.NoError(t, err)
	require.True(t, hasNext)
	val, err = scan.GetInt("val")
	require.NoError(t, err)
	assert.Equal(t, 100, val, "Reader should still see snapshot value")
	scan.Close()

	err = tx1.Commit()
	require.NoError(t, err)
}

func TestMVCC_MultipleUpdates(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_multi_update")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table counter (id int, val int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into counter (id, val) values (1, 0)")
	require.NoError(t, err)

	// Perform multiple sequential updates
	for i := 1; i <= 5; i++ {
		err = db.ExecuteUpdate("update counter set val = " + itoa(i*10) + " where id = 1")
		require.NoError(t, err)
	}

	// Should see final value
	rows, err := db.ExecuteQuery("select val from counter where id = 1")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, 50, rows[0]["val"])

	// Raw scan should show all versions (1 original + 5 updates = 6 physical tuples)
	tx := db.transactionManager.BeginTransaction()
	layout, err := db.metadataManager.GetTableLayout("counter", tx)
	require.NoError(t, err)

	ts, err := table.NewTableScan(tx, layout, "counter")
	require.NoError(t, err)

	rawCount := 0
	for {
		hasNext, err := ts.NextRaw()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		rawCount++
	}
	ts.Close()
	assert.Equal(t, 6, rawCount, "Should have 6 physical tuples (original + 5 update versions)")
	err = tx.Commit()
	require.NoError(t, err)

	// Vacuum should reclaim the 5 dead versions
	tx = db.transactionManager.BeginTransaction()
	commitLog := db.transactionManager.GetCommitLog()
	oldestActive := db.transactionManager.GetOldestActiveTx()
	indexInfo, err := db.metadataManager.GetIndexInfo("counter", tx)
	require.NoError(t, err)

	reclaimed, err := mvcc.Vacuum("counter", layout, tx, commitLog, indexInfo, oldestActive)
	require.NoError(t, err)
	assert.Equal(t, 5, reclaimed, "Vacuum should reclaim 5 dead versions")
	err = tx.Commit()
	require.NoError(t, err)
}

func TestMVCC_VacuumDoesNotReclaimActiveTuples(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_vacuum_safe")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table safe (id int)")
	require.NoError(t, err)
	err = db.ExecuteUpdate("insert into safe (id) values (1)")
	require.NoError(t, err)

	// tx1 starts (holds a snapshot)
	tx1 := db.transactionManager.BeginTransaction()

	// tx2 deletes and commits
	tx2 := db.transactionManager.BeginTransaction()
	_, err = db.planner.ExecuteUpdate("delete from safe where id = 1", tx2)
	require.NoError(t, err)
	err = tx2.Commit()
	require.NoError(t, err)

	// Vacuum while tx1 is still active — should NOT reclaim the deleted tuple
	// because tx1 might still need to see it
	txVac := db.transactionManager.BeginTransaction()
	layout, err := db.metadataManager.GetTableLayout("safe", txVac)
	require.NoError(t, err)
	indexInfo, err := db.metadataManager.GetIndexInfo("safe", txVac)
	require.NoError(t, err)
	commitLog := db.transactionManager.GetCommitLog()
	oldestActive := db.transactionManager.GetOldestActiveTx()

	reclaimed, err := mvcc.Vacuum("safe", layout, txVac, commitLog, indexInfo, oldestActive)
	require.NoError(t, err)
	assert.Equal(t, 0, reclaimed, "Vacuum should not reclaim tuples visible to active transactions")
	err = txVac.Commit()
	require.NoError(t, err)

	// tx1 should still see the row
	plan, err := db.planner.CreatePlan("select id from safe", tx1, nil)
	require.NoError(t, err)
	scan, err := plan.Open()
	require.NoError(t, err)
	err = scan.BeforeFirst()
	require.NoError(t, err)

	hasNext, err := scan.Next()
	require.NoError(t, err)
	assert.True(t, hasNext, "tx1 should still see the deleted row via snapshot")
	scan.Close()

	err = tx1.Commit()
	require.NoError(t, err)
}

func TestMVCC_RecordPageXminXmax(t *testing.T) {
	dbDir := filepath.Join(os.TempDir(), "cranedb_test", "mvcc_record_page")
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	db := NewTestDB(t, dbDir)

	err := db.ExecuteUpdate("create table rp (a int, b varchar(5))")
	require.NoError(t, err)

	tx := db.transactionManager.BeginTransaction()
	layout, err := db.metadataManager.GetTableLayout("rp", tx)
	require.NoError(t, err)

	// Verify MVCC header is included in slot size
	// Schema: a(int=4) + b(string=4+5=9) = 13 data bytes
	// MVCC header: 4(flag) + 8(xmin) + 8(xmax) = 20 bytes
	// Total slot = 20 + 13 = 33 bytes
	expectedSlotSize := record.MVCCHeaderSize + 4 + 4 + 5
	assert.Equal(t, expectedSlotSize, layout.GetSlotSize())

	// Verify field offsets start after MVCC header
	assert.Equal(t, record.MVCCHeaderSize, layout.GetOffset("a"))
	assert.Equal(t, record.MVCCHeaderSize+4, layout.GetOffset("b"))

	err = tx.Commit()
	require.NoError(t, err)
}

// itoa converts an int to a string for building SQL.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	result := ""
	negative := n < 0
	if negative {
		n = -n
	}
	for n > 0 {
		result = string(rune('0'+n%10)) + result
		n /= 10
	}
	if negative {
		result = "-" + result
	}
	return result
}
