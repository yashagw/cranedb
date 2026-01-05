package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/parse"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/plan"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/session"
	"github.com/yashagw/cranedb/internal/transaction"
)

const (
	DefaultPort       = "8080"
	DefaultDBDir      = "./cranedb_data"
	DefaultBlockSize  = 400
	DefaultBufferSize = 40
)

type Server struct {
	fileManager        *file.Manager
	logManager         *dblog.Manager
	bufferManager      *buffer.Manager
	lockTable          *transaction.LockTable
	dirtyPageTable     *buffer.DirtyPageTable
	transactionTable   *transaction.TransactionTable
	transactionManager *transaction.TransactionManager
	metadataManager    *metadata.Manager
	planner            *plan.Planner
}

type QueryResponse struct {
	Type     string                   `json:"type"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Columns  []string                 `json:"columns,omitempty"`
	Affected int                      `json:"affected,omitempty"`
	Error    string                   `json:"error,omitempty"`
	Message  string                   `json:"message,omitempty"`
}

func NewServer(dbDir string) (*Server, error) {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	if err := removeTempTables(dbDir); err != nil {
		return nil, fmt.Errorf("failed to clean temp tables: %w", err)
	}

	fm, err := file.NewManager(dbDir, DefaultBlockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	lm, err := dblog.NewManager(fm, "cranedb.log")
	if err != nil {
		return nil, fmt.Errorf("failed to create log manager: %w", err)
	}

	dirtyPageTable := buffer.NewDirtyPageTable()

	bm, err := buffer.NewManager(fm, lm, dirtyPageTable, DefaultBufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer manager: %w", err)
	}

	lockTable := transaction.NewLockTable()

	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fm, lm, bm, lockTable, dirtyPageTable, transactionTable)

	isNew := true
	metadataFile := filepath.Join(dbDir, "table_catelog.tbl")
	if _, err := os.Stat(metadataFile); err == nil {
		isNew = false
	}

	err = transactionManager.PerformDBRecovery()
	if err != nil {
		return nil, fmt.Errorf("failed to perform recovery: %w", err)
	}

	tx := transactionManager.BeginTransaction()
	md := metadata.NewManager(isNew, tx)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit initial transaction: %w", err)
	}

	queryPlanner := plan.NewBasicQueryPlanner(md)
	updatePlanner := plan.NewBasicUpdatePlanner(md)
	planner := plan.NewPlanner(queryPlanner, updatePlanner)

	return &Server{
		fileManager:        fm,
		logManager:         lm,
		bufferManager:      bm,
		lockTable:          lockTable,
		dirtyPageTable:     dirtyPageTable,
		transactionTable:   transactionTable,
		transactionManager: transactionManager,
		metadataManager:    md,
		planner:            planner,
	}, nil
}

// removeTempTables deletes stale temporary table files (temp*.tbl) left from previous runs.
func removeTempTables(dbDir string) error {
	entries, err := os.ReadDir(dbDir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "temp") && strings.HasSuffix(name, ".tbl") {
			fullPath := filepath.Join(dbDir, name)
			if err := os.Remove(fullPath); err != nil {
				slog.Warn("Failed to remove temp table", "path", fullPath, "err", err)
			}
		}
	}
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	slog.Info("New connection", "remoteAddr", remoteAddr)

	// Create a session for this connection
	sess := session.NewSession()

	defer func() {
		// Rollback any uncommitted transaction on connection close
		if sess.HasActiveTransaction() {
			tx := sess.GetTransaction().(*transaction.Transaction)
			if err := tx.Rollback(); err != nil {
				slog.Error("Error rolling back transaction on connection close", "err", err)
			} else {
				slog.Warn("Transaction rolled back due to connection close", "remoteAddr", remoteAddr)
			}
			sess.ClearTransaction()
		}
		conn.Close()
		slog.Info("Connection closed", "remoteAddr", remoteAddr)
	}()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil && err != io.EOF {
				slog.Error("Error reading from client", "remoteAddr", remoteAddr, "err", err)
			}
			break
		}

		query := strings.TrimSpace(scanner.Text())
		if query == "" {
			continue
		}

		if query == "QUIT" || query == "EXIT" {
			writer.WriteString("Goodbye!\n")
			writer.Flush()
			break
		}

		response := s.executeQuery(query, sess)

		jsonData, err := json.Marshal(response)
		if err != nil {
			errorResp := QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to serialize response: %v", err),
			}
			jsonData, _ = json.Marshal(errorResp)
		}

		writer.Write(jsonData)
		writer.WriteString("\n")
		writer.Flush()
	}
}

func (s *Server) executeQuery(sql string, sess *session.Session) QueryResponse {
	queryPreview := sql
	if len(queryPreview) > 100 {
		queryPreview = queryPreview[:100] + "..."
	}
	slog.Info("Executing query", "query", queryPreview)

	// Check if it's a SET command first
	trimmedSQL := strings.TrimSpace(strings.ToLower(sql))
	if strings.HasPrefix(trimmedSQL, "set ") {
		parser := parse.NewParserFromString(sql)
		setData, err := parser.UpdateCmd()
		if err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to parse SET command: %v", err),
			}
		}

		setCmd, ok := setData.(*parserdata.SetData)
		if !ok {
			return QueryResponse{
				Type:  "error",
				Error: "Invalid SET command",
			}
		}

		// Set the session variable
		sess.SetVariable(setCmd.VariableName(), setCmd.Value())

		// Format the value for display
		var valueStr string
		switch v := setCmd.Value().(type) {
		case bool:
			valueStr = fmt.Sprintf("%t", v)
		case string:
			valueStr = fmt.Sprintf("'%s'", v)
		case int:
			valueStr = fmt.Sprintf("%d", v)
		default:
			valueStr = fmt.Sprintf("%v", v)
		}

		return QueryResponse{
			Type:    "set",
			Message: fmt.Sprintf("%s = %s", setCmd.VariableName(), valueStr),
		}
	}

	// Handle BEGIN command
	if strings.HasPrefix(trimmedSQL, "begin") {
		if sess.HasActiveTransaction() {
			return QueryResponse{
				Type:  "error",
				Error: "there is already a transaction in progress",
			}
		}
		tx := s.transactionManager.BeginTransaction()
		sess.SetTransaction(tx)
		return QueryResponse{
			Type:    "transaction",
			Message: "BEGIN",
		}
	}

	// Handle COMMIT command
	if strings.HasPrefix(trimmedSQL, "commit") {
		if !sess.HasActiveTransaction() {
			return QueryResponse{
				Type:  "error",
				Error: "there is no transaction in progress",
			}
		}
		tx := sess.GetTransaction().(*transaction.Transaction)
		if err := tx.Commit(); err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to commit transaction: %v", err),
			}
		}
		sess.ClearTransaction()
		return QueryResponse{
			Type:    "transaction",
			Message: "COMMIT",
		}
	}

	// Handle ROLLBACK command
	if strings.HasPrefix(trimmedSQL, "rollback") {
		if !sess.HasActiveTransaction() {
			return QueryResponse{
				Type:  "error",
				Error: "there is no transaction in progress",
			}
		}
		tx := sess.GetTransaction().(*transaction.Transaction)
		if err := tx.Rollback(); err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to rollback transaction: %v", err),
			}
		}
		sess.ClearTransaction()
		return QueryResponse{
			Type:    "transaction",
			Message: "ROLLBACK",
		}
	}

	// Determine if we're in an explicit transaction or auto-commit mode
	var tx *transaction.Transaction
	explicitTx := sess.HasActiveTransaction()
	if explicitTx {
		tx = sess.GetTransaction().(*transaction.Transaction)
	} else {
		tx = s.transactionManager.BeginTransaction()
	}
	committed := false
	defer func() {
		// Only auto-rollback/commit if not in an explicit transaction
		if !explicitTx {
			if !committed {
				if err := tx.Rollback(); err != nil {
					slog.Error("Error rolling back transaction", "err", err)
				}
				slog.Warn("Query rolled back", "query", queryPreview)
			} else {
				slog.Info("Query committed", "query", queryPreview)
			}
		}
	}()

	// Check if it's a SELECT query or EXPLAIN by looking at the first keyword
	// This avoids parsing the SQL twice (once here, once in planner methods)
	isQuery := strings.HasPrefix(trimmedSQL, "select")
	isExplain := strings.HasPrefix(trimmedSQL, "explain")

	if isExplain {
		planTree, err := s.planner.ExplainPlan(sql, tx, sess)
		if err != nil {
			return QueryResponse{
				Type:  "error",
				Error: err.Error(),
			}
		}

		// Only auto-commit if not in an explicit transaction
		if !explicitTx {
			if err := tx.Commit(); err != nil {
				return QueryResponse{
					Type:  "error",
					Error: fmt.Sprintf("Failed to commit transaction: %v", err),
				}
			}
			committed = true
		}

		// Return plan tree as a single row with a "plan" column
		return QueryResponse{
			Type:    "explain",
			Rows:    []map[string]interface{}{{"plan": planTree}},
			Columns: []string{"plan"},
		}
	}

	if isQuery {
		queryPlan, err := s.planner.CreatePlan(sql, tx, sess)
		if err != nil {
			return QueryResponse{
				Type:  "error",
				Error: err.Error(),
			}
		}

		queryScan, err := queryPlan.Open()
		if err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to open query plan: %v", err),
			}
		}
		defer queryScan.Close()
		err = queryScan.BeforeFirst()
		if err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to position scan: %v", err),
			}
		}

		schema := queryPlan.Schema()
		columns := append([]string{}, schema.Fields()...)

		rows := []map[string]interface{}{}
		for {
			hasNext, err := queryScan.Next()
			if err != nil {
				queryScan.Close()
				return QueryResponse{
					Type:  "error",
					Error: fmt.Sprintf("Failed to read next record: %v", err),
				}
			}
			if !hasNext {
				break
			}
			row := make(map[string]interface{})
			for _, col := range columns {
				switch schema.Type(col) {
				case record.FieldTypeInt:
					val, err := queryScan.GetInt(col)
					if err != nil {
						queryScan.Close()
						return QueryResponse{
							Type:  "error",
							Error: fmt.Sprintf("Failed to get int value for column %s: %v", col, err),
						}
					}
					row[col] = val
				case record.FieldTypeBool:
					val, err := queryScan.GetBool(col)
					if err != nil {
						queryScan.Close()
						return QueryResponse{
							Type:  "error",
							Error: fmt.Sprintf("Failed to get bool value for column %s: %v", col, err),
						}
					}
					row[col] = val
				case record.FieldTypeString:
					val, err := queryScan.GetString(col)
					if err != nil {
						queryScan.Close()
						return QueryResponse{
							Type:  "error",
							Error: fmt.Sprintf("Failed to get string value for column %s: %v", col, err),
						}
					}
					row[col] = val
				}
			}
			rows = append(rows, row)
		}

		// Only auto-commit if not in an explicit transaction
		if !explicitTx {
			if err := tx.Commit(); err != nil {
				return QueryResponse{
					Type:  "error",
					Error: fmt.Sprintf("Failed to commit transaction: %v", err),
				}
			}
			committed = true
		}

		return QueryResponse{
			Type:    "query",
			Rows:    rows,
			Columns: columns,
		}
	}

	count, err := s.planner.ExecuteUpdate(sql, tx)
	if err != nil {
		slog.Error("Error executing update", "err", err)
		return QueryResponse{
			Type:  "error",
			Error: err.Error(),
		}
	}

	// Only auto-commit if not in an explicit transaction
	if !explicitTx {
		if err := tx.Commit(); err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to commit transaction: %v", err),
			}
		}
		committed = true
	}

	return QueryResponse{
		Type:     "update",
		Affected: count,
	}
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = DefaultPort
	}

	dbDir := os.Getenv("DB_DIR")
	if dbDir == "" {
		dbDir = DefaultDBDir
	}

	server, err := NewServer(dbDir)
	if err != nil {
		slog.Error("Failed to initialize server", "err", err)
		os.Exit(1)
	}

	// Start background buffer flush every 30 seconds
	server.bufferManager.StartBackgroundFlush(30 * time.Second)

	// Start periodic fuzzy checkpoint every 1 minute
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			<-ticker.C
			err := server.transactionManager.PerformCheckpoint()
			if err != nil {
				slog.Error("Error saving checkpoint", "err", err)
			} else {
				slog.Info("Checkpoint saved")
			}
		}
	}()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		slog.Error("Failed to listen on port", "port", port, "err", err)
		os.Exit(1)
	}

	slog.Info("CraneDB server started", "port", port, "dir", dbDir)

	for {
		conn, err := listener.Accept()
		if err != nil {
			slog.Error("Error accepting connection", "err", err)
			continue
		}

		go server.handleConnection(conn)
	}
}
