package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/plan"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// TestDB is a simplified version of the Server struct for testing purposes
type TestDB struct {
	fileManager        *file.Manager
	logManager         *log.Manager
	bufferManager      *buffer.Manager
	transactionManager *transaction.TransactionManager
	metadataManager    *metadata.Manager
	planner            *plan.Planner
}

func NewTestDB(t *testing.T, dbDir string) *TestDB {
	require.NoError(t, os.MkdirAll(dbDir, 0755))

	fm, err := file.NewManager(dbDir, 4096) // Block size 4096
	require.NoError(t, err)

	lm, err := log.NewManager(fm, "cranedb.log")
	require.NoError(t, err)

	dirtyPageTable := buffer.NewDirtyPageTable()
	bm, err := buffer.NewManager(fm, lm, dirtyPageTable, 40) // Buffer size 40
	require.NoError(t, err)

	lockTable := transaction.NewLockTable()
	transactionTable := transaction.NewTransactionTable()
	tm := transaction.NewTransactionManager(fm, lm, bm, lockTable, dirtyPageTable, transactionTable)

	isNew := true
	if _, err := os.Stat(filepath.Join(dbDir, "table_catelog.tbl")); err == nil {
		isNew = false
	}

	// Perform recovery
	err = tm.PerformDBRecovery()
	require.NoError(t, err)

	tx := tm.BeginTransaction()
	md := metadata.NewManager(isNew, tx)
	err = tx.Commit()
	require.NoError(t, err)

	queryPlanner := plan.NewBasicQueryPlanner(md)
	updatePlanner := plan.NewBasicUpdatePlanner(md)
	planner := plan.NewPlanner(queryPlanner, updatePlanner)

	return &TestDB{
		fileManager:        fm,
		logManager:         lm,
		bufferManager:      bm,
		transactionManager: tm,
		metadataManager:    md,
		planner:            planner,
	}
}

func (db *TestDB) ExecuteUpdate(sql string) error {
	tx := db.transactionManager.BeginTransaction()
	defer tx.Rollback() // Safety rollback

	_, err := db.planner.ExecuteUpdate(sql, tx)
	if err != nil {
		return err
	}

	err = tx.Commit()
	return err
}

func (db *TestDB) ExecuteQuery(sql string) ([]map[string]interface{}, error) {
	tx := db.transactionManager.BeginTransaction()
	defer tx.Commit() // Commit for reads too

	plan, err := db.planner.CreatePlan(sql, tx, nil) // passing nil session for now
	if err != nil {
		return nil, err
	}

	scan, err := plan.Open()
	if err != nil {
		return nil, err
	}
	defer scan.Close()

	var results []map[string]interface{}
	schema := plan.Schema()

	if err := scan.BeforeFirst(); err != nil {
		return nil, err
	}

	for {
		hasNext, err := scan.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		row := make(map[string]interface{})
		for _, field := range schema.Fields() {
			// Basic type support based on schema
			switch schema.Type(field) {
			case record.FieldTypeInt:
				val, err := scan.GetInt(field)
				if err != nil {
					return nil, err
				}
				row[field] = val
			case record.FieldTypeString:
				val, err := scan.GetString(field)
				if err != nil {
					return nil, err
				}
				row[field] = val
			case record.FieldTypeBool:
				val, err := scan.GetBool(field)
				if err != nil {
					return nil, err
				}
				row[field] = val
			}
		}
		results = append(results, row)
	}

	return results, nil
}
