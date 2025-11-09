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

func (c *Client) ExecuteQuery(query string) (*QueryResponse, error) {
	if _, err := c.writer.WriteString(query + "\n"); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}
	if err := c.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush query: %w", err)
	}

	responseLine, err := c.reader.ReadString('\n')
	if err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("server closed connection")
		}
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var response QueryResponse
	if err := json.Unmarshal([]byte(strings.TrimSpace(responseLine)), &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}

func printQueryResults(response *QueryResponse) {
	if response.Error != "" {
		fmt.Printf("❌ Error: %s\n\n", response.Error)
		return
	}

	if response.Type == "query" {
		if len(response.Rows) == 0 {
			fmt.Println("(0 rows)")
			fmt.Println()
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
		fmt.Printf("\n(%d row(s))\n\n", len(response.Rows))
	} else if response.Type == "update" {
		fmt.Printf("✓ %d row(s) affected\n\n", response.Affected)
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

	response, err := client.ExecuteQuery(query)
	if err != nil {
		fmt.Printf("❌ Error: %v\n\n", err)
		return false
	}

	printQueryResults(response)
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
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	var queryBuilder strings.Builder

	for {
		if queryBuilder.Len() == 0 {
			fmt.Print("cranedb> ")
		} else {
			fmt.Print("      -> ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}

		if strings.HasSuffix(line, ";") {
			queryBuilder.WriteString(" " + strings.TrimSuffix(line, ";"))
			query := queryBuilder.String()
			queryBuilder.Reset()
			if processQuery(query, client) {
				break
			}
		} else {
			if queryBuilder.Len() > 0 {
				queryBuilder.WriteString(" ")
			}
			queryBuilder.WriteString(line)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}
