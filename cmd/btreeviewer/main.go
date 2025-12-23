package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

const DefaultBlockSize = 400

type viewer struct {
	tx *transaction.Transaction
}

func main() {
	dbDir := flag.String("db", "", "Path to the database directory")
	indexName := flag.String("index", "", "Name of the index to view")

	flag.Parse()

	if *dbDir == "" {
		fmt.Fprintf(os.Stderr, "Usage: %s -db <db_data_dir> [-index <index_name>]\n", os.Args[0])
		os.Exit(1)
	}

	// Disable logging for cleaner output
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	// Initialize database stack
	fm, _ := file.NewManager(*dbDir, DefaultBlockSize)
	lm, _ := dblog.NewManager(fm, "cranedb.log")
	dpt := buffer.NewDirtyPageTable()
	bm, _ := buffer.NewManager(fm, lm, dpt, 200)
	lt := transaction.NewLockTable()
	tt := transaction.NewTransactionTable()
	tm := transaction.NewTransactionManager(fm, lm, bm, lt, dpt, tt)

	// Start a transaction for viewing
	tx := tm.BeginTransaction()
	if tx == nil {
		fmt.Fprintf(os.Stderr, "Error: failed to start transaction\n")
		os.Exit(1)
	}
	defer tx.Commit()

	v := &viewer{tx: tx}

	if *indexName == "" {
		v.listIndices()
		fmt.Println("\nUse -index <name> to open the interactive explorer.")
	} else {
		v.runInteractive(*indexName)
	}
}

func (v *viewer) listIndices() {
	fmt.Println("=== Available Indices ===")
	tm := metadata.NewTableManager(false, v.tx)

	layout, err := tm.GetLayout(metadata.IndexCatalogName, v.tx)
	if err != nil {
		fmt.Printf("Database might be empty or uninitialized (Error: %v)\n", err)
		return
	}

	ts, err := table.NewTableScan(v.tx, layout, metadata.IndexCatalogName)
	if err != nil {
		fmt.Printf("Error: failed to scan index catalog: %v\n", err)
		return
	}
	defer ts.Close()

	count := 0
	for {
		hasNext, err := ts.Next()
		if err != nil || !hasNext {
			break
		}
		idxName, _ := ts.GetString("indexname")
		tblName, _ := ts.GetString("tablename")
		fldName, _ := ts.GetString("fieldname")
		fmt.Printf("- %s (Table: %s, Field: %s)\n", idxName, tblName, fldName)
		count++
	}

	if count == 0 {
		fmt.Println("No indices found.")
	}
}

func (v *viewer) resolveIndex(name string) (*metadata.IndexInfo, *record.Layout, *record.Layout, string, error) {
	tm := metadata.NewTableManager(false, v.tx)
	sm := metadata.NewStatsManager(tm, v.tx)
	im := metadata.NewIndexManager(false, tm, sm, v.tx)

	layout, err := tm.GetLayout(metadata.IndexCatalogName, v.tx)
	if err != nil {
		return nil, nil, nil, "", err
	}

	ts, err := table.NewTableScan(v.tx, layout, metadata.IndexCatalogName)
	if err != nil {
		return nil, nil, nil, "", err
	}
	defer ts.Close()

	var tableName, fieldName string
	found := false
	for {
		hasNext, _ := ts.Next()
		if !hasNext {
			break
		}
		idxName, _ := ts.GetString("indexname")
		if idxName == name {
			tableName, _ = ts.GetString("tablename")
			fieldName, _ = ts.GetString("fieldname")
			found = true
			break
		}
	}

	if !found {
		return nil, nil, nil, "", fmt.Errorf("index %s not found", name)
	}

	iiMap, err := im.GetIndexInfo(tableName, v.tx)
	if err != nil {
		return nil, nil, nil, "", err
	}
	ii, ok := iiMap[fieldName]
	if !ok {
		return nil, nil, nil, "", fmt.Errorf("could not resolve index info for field %s", fieldName)
	}

	leafLayout := ii.CreateIndexLayout()
	dirLayout := ii.CreateIndexLayout()

	return ii, leafLayout, dirLayout, tableName, nil
}

func (v *viewer) getChildTbl(filename string, childIsLeaf bool) string {
	idxName := filename
	if strings.HasSuffix(filename, "dir") {
		idxName = strings.TrimSuffix(filename, "dir")
	} else if strings.HasSuffix(filename, "leaf") {
		idxName = strings.TrimSuffix(filename, "leaf")
	}

	if childIsLeaf {
		return idxName + "leaf"
	}
	return idxName + "dir"
}
