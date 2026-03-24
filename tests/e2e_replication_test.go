package tests

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/plan"
	"github.com/yashagw/cranedb/internal/replication"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestE2EReplication(t *testing.T) {
	// --- Primary setup ---
	primaryDir := filepath.Join(os.TempDir(), "cranedb_test_repl_primary")
	os.RemoveAll(primaryDir)
	defer os.RemoveAll(primaryDir)

	primary := NewTestDB(t, primaryDir)

	// Start WALSender listener on a random port
	primaryListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer primaryListener.Close()
	primaryReplAddr := primaryListener.Addr().String()

	go func() {
		for {
			conn, err := primaryListener.Accept()
			if err != nil {
				return
			}
			ws := replication.NewWALSender(primary.fileManager, primary.logManager, "cranedb.log", conn)
			go func() {
				defer conn.Close()
				ws.Run()
			}()
		}
	}()

	// --- Execute writes on primary ---
	require.NoError(t, primary.ExecuteUpdate("create table users (id int, name varchar(20))"))
	require.NoError(t, primary.ExecuteUpdate("insert into users (id, name) values (1, 'alice')"))
	require.NoError(t, primary.ExecuteUpdate("insert into users (id, name) values (2, 'bob')"))
	require.NoError(t, primary.ExecuteUpdate("insert into users (id, name) values (3, 'charlie')"))

	// Flush primary WAL and buffers to ensure all data is on disk
	require.NoError(t, primary.logManager.ForceFlush())
	require.NoError(t, primary.bufferManager.FlushAllDirtyBuffers())

	primaryLSN := primary.logManager.LatestLSN()
	t.Logf("Primary LSN after writes: %d", primaryLSN)

	// --- Follower setup ---
	followerDir := filepath.Join(os.TempDir(), "cranedb_test_repl_follower")
	os.RemoveAll(followerDir)
	defer os.RemoveAll(followerDir)
	require.NoError(t, os.MkdirAll(followerDir, 0755))

	followerFM, err := file.NewManager(followerDir, 4096)
	require.NoError(t, err)

	followerLM, err := log.NewManager(followerFM, "cranedb.log")
	require.NoError(t, err)

	followerDPT := buffer.NewDirtyPageTable()
	followerBM, err := buffer.NewManager(followerFM, followerLM, followerDPT, 40)
	require.NoError(t, err)

	followerLT := transaction.NewLockTable()
	followerTT := transaction.NewTransactionTable()
	followerTM := transaction.NewTransactionManager(followerFM, followerLM, followerBM, followerLT, followerDPT, followerTT)

	// --- Start WAL receiver ---
	wr := replication.NewWALReceiver(primaryReplAddr, followerFM, followerLM, followerBM, followerDPT, followerTM)
	go func() {
		if err := wr.Run(); err != nil {
			t.Logf("WAL receiver stopped: %v", err)
		}
	}()
	defer wr.Stop()

	// --- Wait for replication to catch up ---
	require.Eventually(t, func() bool {
		return wr.ReplayLSN() >= primaryLSN
	}, 10*time.Second, 100*time.Millisecond, "follower did not catch up to primary LSN %d (current: %d)", primaryLSN, wr.ReplayLSN())

	t.Logf("Follower caught up to LSN: %d", wr.ReplayLSN())

	// Flush follower buffers to ensure replayed data is on disk
	require.NoError(t, followerBM.FlushAllDirtyBuffers())

	// --- Initialize follower metadata and query planner ---
	// The catalog tables were replicated via WAL, so isNew=false
	followerTx := followerTM.BeginTransaction()
	followerMD := metadata.NewManager(false, followerTx)
	require.NoError(t, followerTx.Commit())

	queryPlanner := plan.NewBasicQueryPlanner(followerMD)
	updatePlanner := plan.NewBasicUpdatePlanner(followerMD)
	followerPlanner := plan.NewPlanner(queryPlanner, updatePlanner)

	// --- Query the follower and verify replicated data ---
	followerDB := &TestDB{
		fileManager:        followerFM,
		logManager:         followerLM,
		bufferManager:      followerBM,
		transactionManager: followerTM,
		metadataManager:    followerMD,
		planner:            followerPlanner,
	}

	rows, err := followerDB.ExecuteQuery("select id, name from users")
	require.NoError(t, err)

	// Verify all 3 rows were replicated
	require.Len(t, rows, 3)

	ids := make(map[int]string)
	for _, row := range rows {
		id := row["id"].(int)
		name := row["name"].(string)
		ids[id] = name
	}

	assert.Equal(t, "alice", ids[1])
	assert.Equal(t, "bob", ids[2])
	assert.Equal(t, "charlie", ids[3])
}
