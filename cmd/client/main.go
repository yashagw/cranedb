package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/chzyer/readline"
)

const (
	DefaultHost = "localhost"
	DefaultPort = "8080"
)

type QueryResponse struct {
	Type     string                   `json:"type"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	Columns  []string                 `json:"columns,omitempty"`
	Affected int                      `json:"affected,omitempty"`
	Error    string                   `json:"error,omitempty"`
	Message  string                   `json:"message,omitempty"`
}

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewClient(host, port string) (*Client, error) {
	address := net.JoinHostPort(host, port)
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	return &Client{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) ExecuteQuery(query string) (*QueryResponse, time.Duration, error) {
	start := time.Now()

	if _, err := c.writer.WriteString(query + "\n"); err != nil {
		return nil, 0, fmt.Errorf("failed to send query: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, 0, fmt.Errorf("failed to flush query: %w", err)
	}

	responseLine, err := c.reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return nil, 0, fmt.Errorf("server closed connection")
		}
		return nil, 0, fmt.Errorf("failed to read response: %w", err)
	}

	var response QueryResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(responseLine)), &response); err != nil {
		return nil, 0, fmt.Errorf("failed to parse response: %w", err)
	}

	duration := time.Since(start)
	return &response, duration, nil
}

func printQueryResults(response *QueryResponse, duration time.Duration) {
	if response.Error != "" {
		fmt.Printf("❌ Error: %s\n", response.Error)
		fmt.Printf("⏱️  Time: %v\n\n", duration)
		return
	}

	if response.Type == "set" {
		if response.Message != "" {
			fmt.Printf("✓ %s\n", response.Message)
		} else {
			fmt.Printf("✓ Session variable set successfully\n")
		}
		fmt.Printf("⏱️  Time: %v\n\n", duration)
	} else if response.Type == "explain" {
		// Print the plan tree directly as a multiline string
		if len(response.Rows) > 0 && len(response.Columns) > 0 {
			if plan, ok := response.Rows[0]["plan"].(string); ok {
				fmt.Println(plan)
			} else {
				fmt.Printf("%v\n", response.Rows[0]["plan"])
			}
		}
		fmt.Printf("⏱️  Time: %v\n\n", duration)
	} else if response.Type == "query" {
		if len(response.Rows) == 0 {
			fmt.Println("(0 rows)")
			fmt.Printf("⏱️  Time: %v\n\n", duration)
			return
		}

		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprint(w, strings.Join(response.Columns, "\t"))
		fmt.Fprint(w, "\n")
		fmt.Fprint(w, strings.Repeat("-\t", len(response.Columns)))
		fmt.Fprint(w, "\n")

		for _, row := range response.Rows {
			values := make([]string, len(response.Columns))
			for i, col := range response.Columns {
				val := row[col]
				switch v := val.(type) {
				case float64:
					if v == float64(int64(v)) {
						values[i] = fmt.Sprintf("%d", int64(v))
					} else {
						values[i] = fmt.Sprintf("%g", v)
					}
				case string:
					values[i] = v
				case int:
					values[i] = fmt.Sprintf("%d", v)
				default:
					values[i] = fmt.Sprintf("%v", v)
				}
			}
			fmt.Fprint(w, strings.Join(values, "\t"))
			fmt.Fprint(w, "\n")
		}
		w.Flush()
		fmt.Printf("\n(%d row(s))\n", len(response.Rows))
		fmt.Printf("⏱️  Time: %v\n\n", duration)
	} else if response.Type == "update" {
		fmt.Printf("✓ %d row(s) affected\n", response.Affected)
		fmt.Printf("⏱️  Time: %v\n\n", duration)
	}
}

// processQuery processes a query string: executes it and prints results.
// Returns true if the client should exit (QUIT/EXIT command).
func processQuery(query string, client *Client) bool {
	query = strings.TrimSpace(query)
	if query == "" {
		return false
	}

	upperQuery := strings.ToUpper(query)
	if upperQuery == "QUIT" || upperQuery == "EXIT" {
		fmt.Println("Goodbye!")
		return true
	}

	response, duration, err := client.ExecuteQuery(query)
	if err != nil {
		fmt.Printf("❌ Error: %v\n\n", err)
		return false
	}

	printQueryResults(response, duration)
	return false
}

func main() {
	host := os.Getenv("CRANEDB_HOST")
	if host == "" {
		host = DefaultHost
	}

	port := os.Getenv("CRANEDB_PORT")
	if port == "" {
		port = DefaultPort
	}

	client, err := NewClient(host, port)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error connecting to server: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	fmt.Println("🐦 CraneDB Client")
	fmt.Printf("Connected to %s:%s\n", host, port)
	fmt.Println("Type 'QUIT' or 'EXIT' to exit, or enter SQL queries")
	fmt.Println("Use 'SET variable_name = value' to set session variables (e.g., SET no_materialize = true)")
	fmt.Println("Use ↑/↓ arrow keys to navigate command history")
	fmt.Println()

	// Create readline instance with history support
	historyFile := ""
	if home := os.Getenv("HOME"); home != "" {
		historyFile = home + "/.cranedb_history"
	} else if home := os.Getenv("USERPROFILE"); home != "" {
		// Windows
		historyFile = home + "\\.cranedb_history"
	}

	rl, err := readline.NewEx(&readline.Config{
		Prompt:          "cranedb> ",
		HistoryFile:     historyFile,
		HistoryLimit:    1000,
		AutoComplete:    nil,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error initializing readline: %v\n", err)
		os.Exit(1)
	}
	defer rl.Close()

	var queryBuilder strings.Builder

	for {
		// Update prompt based on whether we're building a multi-line query
		if queryBuilder.Len() == 0 {
			rl.SetPrompt("cranedb> ")
		} else {
			rl.SetPrompt("      -> ")
		}

		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				// Ctrl+C - clear current query builder
				queryBuilder.Reset()
				fmt.Println()
				continue
			} else if err == io.EOF {
				// Ctrl+D - exit
				break
			}
			fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
			break
		}

		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		// Check for QUIT/EXIT commands (with or without semicolon)
		lineWithoutSemicolon := strings.TrimSuffix(line, ";")
		lineWithoutSemicolon = strings.TrimSpace(lineWithoutSemicolon)
		upperLine := strings.ToUpper(lineWithoutSemicolon)
		if upperLine == "QUIT" || upperLine == "EXIT" {
			fmt.Println("Goodbye!")
			break
		}

		// Build multi-line query
		if queryBuilder.Len() > 0 {
			queryBuilder.WriteString(" ")
		}
		queryBuilder.WriteString(line)

		// If line ends with semicolon, execute the query
		if strings.HasSuffix(line, ";") {
			fullQuery := strings.TrimSpace(queryBuilder.String())
			queryBuilder.Reset()

			// Save to history with semicolon so it's preserved when navigating history
			rl.SaveHistory(fullQuery)

			// Remove trailing semicolon for execution
			query := strings.TrimSuffix(fullQuery, ";")
			query = strings.TrimSpace(query)

			if query == "" {
				continue
			}

			if processQuery(query, client) {
				break
			}
		}
	}
}
