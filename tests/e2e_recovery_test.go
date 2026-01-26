package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/failpoint"
)

// runRecoveryTest runs a recovery test with the given setup function, failpoint name, and failpoint value.
func runRecoveryTest(t *testing.T, testName string,
	setupFn func(*TestDB),
	failpointName string,
	failpointValue string,
	verifyFn func(*TestDB)) {

	dbDir := filepath.Join(os.TempDir(), "cranedb_test", testName)
	os.RemoveAll(dbDir)
	defer os.RemoveAll(dbDir)

	// Track if failpoint was hit
	failpointHit := false
	panicMessage := ""

	// Phase 1: Setup and Crash
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Expected crash
				panicMessage = fmt.Sprintf("%v", r)
				fmt.Printf("Recovered from panic: %v\n", r)

				// Verify the panic was from the expected failpoint
				if failpointName != "" {
					if panicStr, ok := r.(string); ok {
						expectedPanic := "failpoint: crash " + failpointName
						assert.Equal(t, expectedPanic, panicStr, "Panic message should match expected failpoint")
						failpointHit = true
					}
				}
			}
		}()

		db := NewTestDB(t, dbDir)

		err := db.ExecuteUpdate("create table test (a int, b varchar(20))")
		require.NoError(t, err)

		// Flush all dirty buffers before enabling failpoint and running the setup function
		err = db.bufferManager.FlushAllDirtyBuffers()
		require.NoError(t, err)

		// Enable failpoint BEFORE setupFn (which may trigger it)
		if failpointName != "" {
			failpoint.Enable(failpointName, failpointValue)
			defer failpoint.Disable(failpointName)
		}

		setupFn(db)
	}()

	// Verify failpoint was hit if one was specified
	if failpointName != "" {
		assert.True(t, failpointHit, "Failpoint '%s' should have been triggered. Panic message: %s", failpointName, panicMessage)
	}

	// Ensure failpoint is disabled for recovery
	failpoint.Disable(failpointName)

	// Phase 2: Recovery and Verification
	db := NewTestDB(t, dbDir)
	verifyFn(db)
}

// TestDurabilityCrashAfterCommitFlush verifies durability of committed transactions when a crash
// occurs before the modified data pages are flushed to disk.
//
// Scenario:
// 1. Transaction inserts data and commits.
// 2. Commit log record is flushed to disk (WAL protocol).
// 3. Buffer flush is triggered for the dirty data page.
// 4. Crash occurs before the data page is written to disk.
//
// Recovery Expectation:
// - Analysis pass identifies the transaction as committed.
// - Redo pass compares the log record LSN with the persistent PageLSN on disk.
// - Since the page was not written (PageLSN < RecordLSN), the redo operation is performed.
// - The data is restored and durable.
func TestDurabilityCrashAfterCommitFlush(t *testing.T) {
	runRecoveryTest(t, "durability_after_commit",
		func(db *TestDB) {
			err := db.ExecuteUpdate("insert into test (a, b) values (1, 'one')")
			require.NoError(t, err)

			dirtyCount := db.bufferManager.CountDirtyBuffers()
			assert.Greater(t, dirtyCount, 0, "There should be dirty buffers to flush after insert")

			// Force flush all dirty buffers to trigger the failpoint
			// This should panic with "failpoint: crash before buffer write"
			err = db.bufferManager.FlushAllDirtyBuffers()
			require.NoError(t, err)
		},
		"before-buffer-write-to-disk",
		"",
		func(db *TestDB) {
			rows, err := db.ExecuteQuery("select a from test")
			require.NoError(t, err)
			assert.Len(t, rows, 1, "Transaction should be redone")
		},
	)
}

// TestConcurrentWinnerAndLoser verifies recovery handling of concurrent transactions where one
// commits (winner) and another crashes while active (loser).
//
// Scenario:
// 1. Tx1 inserts data and commits successfully.
// 2. Tx2 performs updates but is still active (uncommitted).
// 3. Crash occurs.
//
// Recovery Expectation:
// - Tx1 is identified as a winner; its updates are preserved (redone if necessary).
// - Tx2 is identified as a loser (inflight at crash); its updates are undone.
// - The database state reflects only Tx1's changes.
func TestConcurrentWinnerAndLoser(t *testing.T) {
	runRecoveryTest(t, "concurrent_winner_loser",
		func(db *TestDB) {
			// Tx1 commits successfully.
			err := db.ExecuteUpdate("insert into test (a, b) values (1, 'winner')")
			require.NoError(t, err)

			// Tx2 starts but doesn't commit. We need manual transaction control here
			// to avoid the auto-commit behavior of ExecuteUpdate.
			tx2 := db.transactionManager.BeginTransaction()
			_, err = db.planner.ExecuteUpdate("insert into test (a, b) values (2, 'loser')", tx2)
			require.NoError(t, err)

			// Simulate a crash while Tx2 is active.
			// Since we haven't committed Tx2, it should be undone during recovery.
			panic("crash with active transaction")
		},
		"",
		"",
		func(db *TestDB) {
			rows, err := db.ExecuteQuery("select b from test")
			require.NoError(t, err)

			foundWinner := false
			foundLoser := false
			for _, row := range rows {
				if row["b"] == "winner" {
					foundWinner = true
				}
				if row["b"] == "loser" {
					foundLoser = true
				}
			}

			assert.True(t, foundWinner, "Winner tx should be present")
			assert.False(t, foundLoser, "Loser tx should be undone")
		},
	)
}

// TestCrashDuringCheckpoint verifies recovery behavior when a crash occurs during the checkpointing process.
//
// Scenario:
// 1. Transaction performs updates and remains active.
// 2. Checkpoint process is initiated.
// 3. Crash occurs before the checkpoint log record is flushed to disk.
//
// Recovery Expectation:
// - The incomplete checkpoint is ignored/not found.
// - Recovery starts from the last valid checkpoint (or start of log).
// - The active transaction is identified as a loser and rolled back.
func TestCrashDuringCheckpoint(t *testing.T) {
	runRecoveryTest(t, "crash_during_checkpoint",
		func(db *TestDB) {
			// Tx1 Active
			tx1 := db.transactionManager.BeginTransaction()
			_, err := db.planner.ExecuteUpdate("insert into test (a, b) values (1, 'active')", tx1)
			require.NoError(t, err)

			// Trigger Checkpoint
			// Checkpoint is usually background. We can call it manually.
			err = db.transactionManager.PerformCheckpoint()
			require.NoError(t, err)
		},
		"before-checkpoint-log-flush",
		"",
		func(db *TestDB) {
			rows, err := db.ExecuteQuery("select b from test")
			require.NoError(t, err)
			assert.Empty(t, rows, "Active transaction should be undone")
		},
	)
}

// TestCrashBeforeCommitLogFlush verifies recovery behavior when a crash occurs after a commit is requested
// but before the commit log record is flushed to disk.
//
// Scenario:
// 1. Transaction inserts data.
// 2. Commit is called.
// 3. Crash occurs internally during commit, before the commit record is successfully flushed to the log file.
//
// Recovery Expectation:
// - The transaction is treated as uncommitted (loser) because the commit record is missing from the log.
// - Any changes made by the transaction are undone.
// - The transaction is effectively lost.
func TestCrashBeforeCommitLogFlush(t *testing.T) {
	runRecoveryTest(t, "crash_before_commit_log_flush",
		func(db *TestDB) {
			// We manually execute a transaction to have fine-grained control
			tx := db.transactionManager.BeginTransaction()

			// Use simple direct update to skip planner complexity for this focused test if needed,
			// but planner is fine.
			_, err := db.planner.ExecuteUpdate("insert into test (a, b) values (1, 'lost')", tx)
			require.NoError(t, err)

			// Commit should fail/panic before flushing the log
			err = tx.Commit()
			require.NoError(t, err)
		},
		"before-commit-log-flush",
		"",
		func(db *TestDB) {
			rows, err := db.ExecuteQuery("select b from test")
			require.NoError(t, err)
			// Since commit log record wasn't flushed, this tx is a loser.
			// It should be completely undone/invisible.
			assert.Empty(t, rows, "Transaction committed but log not flushed should be lost")
		},
	)
}

// TestCrashBeforeBufferRemoveFromDPT verifies recovery behavior when a crash occurs
// after a dirty page is flushed to disk but before it is removed from the Dirty Page Table.
//
// Scenario:
// 1. Transaction inserts data and makes a page dirty.
// 2. Buffer flush is triggered.
// 3. Page is successfully written to disk (PageLSN updated).
// 4. Crash occurs before the page is removed from the Dirty Page Table in memory.
//
// Recovery Expectation:
// - Recovery analysis rebuilds the DPT from logs, identifying the page as potentially dirty.
// - Redo pass compares the log record LSN with the persistent PageLSN on disk.
// - Since the page was successfully written (PageLSN >= RecordLSN), the redo operation is skipped (ARIES optimization).
// - The data remains consistent and persistent.
func TestCrashBeforeBufferRemoveFromDPT(t *testing.T) {
	runRecoveryTest(t, "crash_before_buffer_remove_from_dpt",
		func(db *TestDB) {
			err := db.ExecuteUpdate("insert into test (a, b) values (1, 'persistent')")
			require.NoError(t, err)

			// Now force flush buffers.
			// Failpoint is inside buffer.flush(), AFTER writing to disk, BEFORE removing from DPT.
			// Note: FlushAllDirtyBuffers loops through buffers. If one panics, the rest might not flush, but that's fine.
			err = db.bufferManager.FlushAllDirtyBuffers()
			require.NoError(t, err)
		},
		"before-buffer-remove-from-dirty-page-table",
		"file=test.tbl",
		func(db *TestDB) {
			rows, err := db.ExecuteQuery("select b from test")
			require.NoError(t, err)
			assert.Len(t, rows, 1, "Data should be present as it was safely written to disk")
			assert.Equal(t, "persistent", rows[0]["b"])
		},
	)
}

// TestCrashDuringRollback verifies recovery behavior when a crash occurs during the rollback phase.
//
// Scenario:
// 1. Transaction performs updates.
// 2. Rollback is initiated.
// 3. Undo operations are performed, and CLRs are logged.
// 4. Crash occurs before the final 'Rollback' log record is flushed to disk.
//
// Recovery Expectation:
//   - Analysis pass identifies the transaction as active (neither committed nor rolled back).
//   - Undo pass identifies it as a loser transaction.
//   - Recovery continues the undo process. ARIES ensures idempotency using CLRs (Compensation Log Records)
//     to skip already undone operations, ensuring the transaction is fully rolled back.
func TestCrashDuringRollback(t *testing.T) {
	runRecoveryTest(t, "crash_during_rollback",
		func(db *TestDB) {
			tx := db.transactionManager.BeginTransaction()
			_, err := db.planner.ExecuteUpdate("insert into test (a, b) values (1, 'rolled_back')", tx)
			require.NoError(t, err)

			// Manually rollback.
			// This will inject panic "before-rollback-log-flush"
			err = tx.Rollback()
			require.NoError(t, err)
		},
		"before-rollback-log-flush",
		"",
		func(db *TestDB) {
			// The transaction should have been effectively rolled back (either by the first attempt + crash, or by recovery)
			rows, err := db.ExecuteQuery("select b from test where b = 'rolled_back'")
			require.NoError(t, err)
			assert.Empty(t, rows, "Transaction should be rolled back completely")

			// Verify database is usable
			err = db.ExecuteUpdate("insert into test (a, b) values (2, 'new')")
			require.NoError(t, err)
			rows, err = db.ExecuteQuery("select b from test")
			require.NoError(t, err)
			assert.Len(t, rows, 1)
			assert.Equal(t, "new", rows[0]["b"])
		},
	)
}
