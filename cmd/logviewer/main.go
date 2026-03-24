package main

import (
	"fmt"
	"os"
	"path/filepath"
	"text/tabwriter"

	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/transaction"
)

const DefaultBlockSize = 400

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <log_file_path>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Example: %s ./cranedb_data/cranedb.log\n", os.Args[0])
		os.Exit(1)
	}

	logFilePath := os.Args[1]

	// Extract directory and filename from the log file path
	logDir := filepath.Dir(logFilePath)
	logFilename := filepath.Base(logFilePath)

	// Check if log file exists
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "Error: log file does not exist: %s\n", logFilePath)
		os.Exit(1)
	}

	// Initialize file manager
	fm, err := file.NewManager(logDir, DefaultBlockSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create file manager: %v\n", err)
		os.Exit(1)
	}

	// Initialize log manager
	lm, err := dblog.NewManager(fm, logFilename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create log manager: %v\n", err)
		os.Exit(1)
	}
	defer lm.Close()

	// Get iterator
	iter, err := lm.BackwardIterator()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: failed to create log iterator: %v\n", err)
		os.Exit(1)
	}

	// Print header
	fmt.Printf("=== CraneDB Log Viewer ===\n")
	fmt.Printf("Log file: %s\n", logFilePath)
	fmt.Printf("========================================\n\n")

	// Create tabwriter for table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	// Print table header
	fmt.Fprintln(w, "LSN\tType\tTx\tPrevLSN\tBlock\tOffset\tOld Value\tNew Value")
	fmt.Fprintln(w, "---\t----\t--\t-------\t-----\t------\t---------\t---------")

	recordCount := 0

	// Iterate through all log records
	for iter.HasNext() {
		logBytes := iter.Next()
		if logBytes == nil {
			break
		}

		record, err := transaction.CreateLogRecord(logBytes)
		if err != nil {
			fmt.Printf("[CORRUPTED] Error parsing log record: %v\n", err)
			continue
		}

		recordCount++

		// Format record as table row
		var tx, lsn, prevLSN, block, offset, oldVal, newVal string

		switch rec := record.(type) {
		case *transaction.CheckpointLogRecord:
			tx = "-"
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = "-"
			block = "-"
			offset = "-"
			oldVal = "-"
			newVal = "-"

		case *transaction.StartLogRecord:
			tx = fmt.Sprintf("%d", rec.TxNumber())
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = fmt.Sprintf("%d", rec.PrevLSN())
			block = "-"
			offset = "-"
			oldVal = "-"
			newVal = "-"

		case *transaction.CommitLogRecord:
			tx = fmt.Sprintf("%d", rec.TxNumber())
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = fmt.Sprintf("%d", rec.PrevLSN())
			block = "-"
			offset = "-"
			oldVal = "-"
			newVal = "-"

		case *transaction.RollbackLogRecord:
			tx = fmt.Sprintf("%d", rec.TxNumber())
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = fmt.Sprintf("%d", rec.PrevLSN())
			block = "-"
			offset = "-"
			oldVal = "-"
			newVal = "-"

		case *transaction.SetIntLogRecord:
			tx = fmt.Sprintf("%d", rec.TxNumber())
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = fmt.Sprintf("%d", rec.PrevLSN())
			block = fmt.Sprintf("%s[%d]", rec.Block().Filename(), rec.Block().Number())
			offset = fmt.Sprintf("%d", rec.Offset())
			oldVal = fmt.Sprintf("%d", rec.OldValue())
			newVal = fmt.Sprintf("%d", rec.NewValue())

		case *transaction.SetStringLogRecord:
			tx = fmt.Sprintf("%d", rec.TxNumber())
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = fmt.Sprintf("%d", rec.PrevLSN())
			block = fmt.Sprintf("%s[%d]", rec.Block().Filename(), rec.Block().Number())
			offset = fmt.Sprintf("%d", rec.Offset())
			oldVal = fmt.Sprintf("%q", rec.OldValue())
			newVal = fmt.Sprintf("%q", rec.NewValue())

		case *transaction.SetBoolLogRecord:
			tx = fmt.Sprintf("%d", rec.TxNumber())
			lsn = fmt.Sprintf("%d", rec.LSN())
			prevLSN = fmt.Sprintf("%d", rec.PrevLSN())
			block = fmt.Sprintf("%s[%d]", rec.Block().Filename(), rec.Block().Number())
			offset = fmt.Sprintf("%d", rec.Offset())
			oldVal = fmt.Sprintf("%t", rec.OldValue())
			newVal = fmt.Sprintf("%t", rec.NewValue())
		}

		recordType := getLogRecordTypeName(record.Op())
		fmt.Fprint(w, lsn, "\t", recordType, "\t", tx, "\t", prevLSN, "\t", block, "\t", offset, "\t", oldVal, "\t", newVal, "\n")
	}

	w.Flush()

	if recordCount == 0 {
		fmt.Println("\nNo log records found in the log file.")
	} else {
		fmt.Printf("\n========================================\n")
		fmt.Printf("Total records: %d\n", recordCount)
	}
}

func getLogRecordTypeName(op transaction.LogRecordType) string {
	switch op {
	case transaction.LogRecordCheckpoint:
		return "CHECKPOINT"
	case transaction.LogRecordStart:
		return "START"
	case transaction.LogRecordCommit:
		return "COMMIT"
	case transaction.LogRecordRollback:
		return "ROLLBACK"
	case transaction.LogRecordSetInt:
		return "SET_INT"
	case transaction.LogRecordSetString:
		return "SET_STRING"
	case transaction.LogRecordSetBool:
		return "SET_BOOL"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", op)
	}
}
