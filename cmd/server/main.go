package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	dblog "github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/plan"
	"github.com/yashagw/cranedb/internal/transaction"
)

const (
	DefaultPort       = "8080"
	DefaultDBDir      = "./cranedb_data"
	DefaultBlockSize  = 400
	DefaultBufferSize = 20
)

type Server struct {
	fileManager     *file.Manager
	logManager      *dblog.Manager
	bufferManager   *buffer.Manager
	lockTable       *transaction.LockTable
	metadataManager *metadata.Manager
	planner         *plan.Planner
}

type QueryResponse struct {
	Type     string                   `json:"type"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Columns  []string                 `json:"columns,omitempty"`
	Affected int                      `json:"affected,omitempty"`
	Error    string                   `json:"error,omitempty"`
}

func NewServer(dbDir string) (*Server, error) {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create database directory: %w", err)
	}

	fm, err := file.NewManager(dbDir, DefaultBlockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}

	lm, err := dblog.NewManager(fm, "cranedb.log")
	if err != nil {
		return nil, fmt.Errorf("failed to create log manager: %w", err)
	}

	bm, err := buffer.NewManager(fm, lm, DefaultBufferSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create buffer manager: %w", err)
	}

	lockTable := transaction.NewLockTable()

	isNew := true
	metadataFile := filepath.Join(dbDir, "tables.tbl")
	if _, err := os.Stat(metadataFile); err == nil {
		isNew = false
	}

	tx := transaction.NewTransaction(fm, lm, bm, lockTable)
	err = tx.DoRecovery()
	if err != nil {
		return nil, fmt.Errorf("failed to perform recovery: %w", err)
	}

	md := metadata.NewManager(isNew, tx)
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit initial transaction: %w", err)
	}

	queryPlanner := plan.NewBasicQueryPlanner(md)
	updatePlanner := plan.NewBasicUpdatePlanner(md)
	planner := plan.NewPlanner(queryPlanner, updatePlanner)

	return &Server{
		fileManager:     fm,
		logManager:      lm,
		bufferManager:   bm,
		lockTable:       lockTable,
		metadataManager: md,
		planner:         planner,
	}, nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	for {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil && err != io.EOF {
				log.Printf("Error reading from client: %v", err)
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

		response := s.executeQuery(query)

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

func (s *Server) executeQuery(sql string) QueryResponse {
	tx := transaction.NewTransaction(s.fileManager, s.logManager, s.bufferManager, s.lockTable)
	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(); err != nil {
				log.Printf("Error rolling back transaction: %v", err)
			}
		}
	}()

	// Check if it's a SELECT query by looking at the first keyword
	// This avoids parsing the SQL twice (once here, once in planner methods)
	trimmedSQL := strings.TrimSpace(strings.ToLower(sql))
	isQuery := strings.HasPrefix(trimmedSQL, "select")

	if isQuery {
		queryPlan, err := s.planner.CreatePlan(sql, tx)
		if err != nil {
			return QueryResponse{
				Type:  "error",
				Error: err.Error(),
			}
		}

		queryScan := queryPlan.Open()
		defer queryScan.Close()
		queryScan.BeforeFirst()

		schema := queryPlan.Schema()
		columns := append([]string{}, schema.Fields()...)

		rows := []map[string]interface{}{}
		for queryScan.Next() {
			row := make(map[string]interface{})
			for _, col := range columns {
				if schema.Type(col) == "int" {
					row[col] = queryScan.GetInt(col)
				} else {
					row[col] = queryScan.GetString(col)
				}
			}
			rows = append(rows, row)
		}

		if err := tx.Commit(); err != nil {
			return QueryResponse{
				Type:  "error",
				Error: fmt.Sprintf("Failed to commit transaction: %v", err),
			}
		}
		committed = true

		return QueryResponse{
			Type:    "query",
			Rows:    rows,
			Columns: columns,
		}
	}

	count, err := s.planner.ExecuteUpdate(sql, tx)
	if err != nil {
		return QueryResponse{
			Type:  "error",
			Error: err.Error(),
		}
	}

	if err := tx.Commit(); err != nil {
		return QueryResponse{
			Type:  "error",
			Error: fmt.Sprintf("Failed to commit transaction: %v", err),
		}
	}
	committed = true

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
		log.Fatalf("Failed to initialize server: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	log.Printf("CraneDB server listening on port %s", port)
	log.Printf("Database directory: %s", dbDir)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		go server.handleConnection(conn)
	}
}
