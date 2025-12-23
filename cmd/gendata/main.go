package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"time"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/plan"
	"github.com/yashagw/cranedb/internal/session"
	"github.com/yashagw/cranedb/internal/transaction"
)

func randomString(n int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	dbDir := flag.String("db", "./test_data", "Path to the database directory")
	count := flag.Int("count", 100, "Number of records to insert")
	tableName := flag.String("table", "users", "Name of the table to create")
	random := flag.Bool("random", true, "Randomize IDs and data")
	blockSize := flag.Int("blocksize", 400, "Block size for the database")
	flag.Parse()

	// Clean up old data
	os.RemoveAll(*dbDir)
	if err := os.MkdirAll(*dbDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create directory: %v\n", err)
		os.Exit(1)
	}

	// Disable noise
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	fm, _ := file.NewManager(*dbDir, *blockSize)
	lm, _ := dblog.NewManager(fm, "cranedb.log")
	dpt := buffer.NewDirtyPageTable()
	bm, _ := buffer.NewManager(fm, lm, dpt, 10000) // Large buffer for speed
	lt := transaction.NewLockTable()
	tt := transaction.NewTransactionTable()
	tm := transaction.NewTransactionManager(fm, lm, bm, lt, dpt, tt)

	// Perform recovery (creates necessary tables if they don't exist)
	_ = tm.PerformDBRecovery()
	bm.StartBackgroundFlush(100 * time.Millisecond)
	defer bm.StopBackgroundFlush()

	// Initialize Metadata Manager
	tx := tm.BeginTransaction()
	mdm := metadata.NewManager(true, tx)
	tx.Commit()

	// Initialize Planner
	qp := plan.NewBasicQueryPlanner(mdm)
	up := plan.NewBasicUpdatePlanner(mdm)
	planner := plan.NewPlanner(qp, up)

	sess := session.NewSession()

	fmt.Printf("Generating %d records in %s/%s (random=%v) using SQL...\n", *count, *dbDir, *tableName, *random)

	// 1. Create table
	createTableSQL := fmt.Sprintf("CREATE TABLE %s (id INT, name VARCHAR(10))", *tableName)
	executeSQL(planner, tm, createTableSQL, sess)

	// 2. Create indices
	createIDIndexSQL := fmt.Sprintf("CREATE INDEX idx_id ON %s (id)", *tableName)
	executeSQL(planner, tm, createIDIndexSQL, sess)

	createNameIndexSQL := fmt.Sprintf("CREATE INDEX idx_name ON %s (name)", *tableName)
	executeSQL(planner, tm, createNameIndexSQL, sess)

	// 3. Insert records
	ids := make([]int, 0, *count)
	if *random {
		rand.Seed(time.Now().UnixNano())
		seen := make(map[int]bool)
		for len(ids) < *count {
			id := rand.Intn(*count * 10)
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	} else {
		for i := 0; i < *count; i++ {
			ids = append(ids, i+1)
		}
	}

	for _, id := range ids {
		name := fmt.Sprintf("user-%d", id)
		if *random {
			name = randomString(10)
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s (id, name) VALUES (%d, '%s')", *tableName, id, name)
		executeSQL(planner, tm, insertSQL, sess)
	}

	bm.FlushAllDirtyBuffers()

	fmt.Println("Successfully generated sample data.")
}

func executeSQL(p *plan.Planner, tm *transaction.TransactionManager, sql string, sess *session.Session) {
	tx := tm.BeginTransaction()
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	_, err := p.ExecuteUpdate(sql, tx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error executing SQL (%s): %v\n", sql, err)
		os.Exit(1)
	}

	err = tx.Commit()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error committing transaction for SQL (%s): %v\n", sql, err)
		os.Exit(1)
	}
	tx = nil
}
