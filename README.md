# CraneDB

A relational database implementation written in Go. While the initial design follows the principles and concepts from the "Database Design and Implementation" book, it has been significantly enhanced with advanced features like MVCC (Multi-Version Concurrency Control), ARIES recovery, fuzzy checkpointing, and more.

## About

This project is an educational implementation of a relational database management system (RDBMS) from scratch. The goal is to understand the fundamental concepts and internals of database systems by implementing core components

**Note**: This project is not intended for production use and serves as an educational implementation of database internals.

## Getting Started

### Build

```bash
git clone https://github.com/yashagw/cranedb.git
cd cranedb
make build
```

### Run Server

```bash
make run-server
```

Server starts on port `8080` by default.

To use a different port and database directory:

```bash
PORT=8082 DB_DIR=./cranedb_data make run-server
```

### Run Client

In a new terminal:

```bash
make run-client
```

To connect to a different port:

```bash
CRANEDB_PORT=8082 make run-client
```

**Note**: Exit client by typing `QUIT` or pressing Ctrl+C.

## Features

### Core Storage Engine

- **File Management**: Fixed-size block storage with efficient read/write operations
- **Page Management**: In-memory page abstraction with binary data serialization
- **Buffer Pool**: LRU-style buffer management with pin/unpin mechanism, timeout handling, and background flush for dirty buffers

### Transaction Management

- **ACID Properties**: Transaction support with atomicity (commit/rollback), durability (write-ahead logging), snapshot isolation (MVCC), and transaction-level consistency
- **MVCC (Multi-Version Concurrency Control)**: 
  - **Snapshot Isolation**: Each transaction sees a consistent point-in-time snapshot of the database
  - **Non-blocking Reads**: Readers never block writers, and writers never block readers
  - **Version Management**: Tuples maintain xmin (creator transaction) and xmax (deleter transaction) timestamps
  - **Visibility Rules**: Automatic visibility determination based on transaction snapshots and commit status
  - **Write-Write Conflict Detection**: First-writer-wins conflict resolution for concurrent updates
  - **Update Semantics**: UPDATE operations create new tuple versions (delete old + insert new) rather than in-place modifications
  - **Delete Semantics**: DELETE operations mark tuples with xmax rather than physically removing them
- **Concurrency Control**: Exclusive locks for writers; readers use MVCC snapshots without acquiring locks
- **Deadlock Prevention**: Timeout-based lock management to prevent indefinite waiting
- **Recovery**: ARIES recovery algorithm (Analysis, Redo, Undo) for advanced crash recovery and transaction rollback
- **Vacuum**: Automatic garbage collection of dead tuple versions that are no longer visible to any active transaction

### Logging and Recovery

- **Log Records**: Support for checkpoint, start, commit, rollback, data modification, and compensation records (CLRs)
- **Crash Recovery**: Automatic recovery from system crashes using ARIES (redo all necessary changes and undo uncommitted transactions)
- **Corruption Detection**: CRC32 checksum verification for log records to ensure data integrity during recovery
- **Checkpoints**: Periodic fuzzy checkpointing to reduce recovery time by flushing dirty pages and logging system state

### Record Management

- **Schema Support**: Dynamic schema definition with integer, boolean, and string field types
- **Record Layout**: Efficient record storage with offset-based field access
- **Table Scanning**: Iterator-based table scanning with insert, update, and delete operations
- **Record Identification**: Unique RID (Record ID) system for record addressing

### Metadata Management

- **Table Management**: Create and query table metadata
- **View Management**: Virtual table support with view definition storage
- **Index Management**: Index metadata tracking and index creation
- **Statistics**: Table statistics collection for cost estimation

### Query Processing

- **SQL Parser**: Lexical analysis and parsing of SQL statements
- **Query Planning**: Execution plan generation with cost estimation and index-aware optimization
- **Query Execution**: Iterator-based query execution with lazy evaluation
- **Expression and Predicate Evaluation**: Support for field references, constant values, and WHERE clause filtering with all comparison operators (=, !=, >, <, >=, <=) and AND/OR conditions
- **Sorting**: ORDER BY clause support using external merge sort for sorting query results by one or more fields
- **Grouping**: GROUP BY clause support for grouping records by specified fields
- **Aggregation Functions**: Support for MAX, MIN, COUNT, SUM, and DISTINCT aggregation functions in GROUP BY queries
- **Update Operations**: Execution of INSERT, UPDATE, and DELETE statements with predicate support and automatic index maintenance
- **Materialization**: Temporary table materialization for query optimization, especially beneficial for nested loop joins
- **B-tree Indexes**: Efficient B-tree index implementation for fast lookups and range queries
- **Limit and Offset**: LIMIT and OFFSET clauses support for result pagination
- **EXPLAIN**: Query plan visualization to understand execution strategies

### Reliability and Testing

- **Failpoints**: Injectable failpoints for simulating system crashes at specific points to verify recovery correctness
- **End-to-End Recovery Tests**: Automated tests that simulate random crashes and verify durability and consistency

## SQL Reference

### Data Types

- `INT` - 32-bit integer
- `VARCHAR(n)` - Variable-length string with maximum length n
- `BOOL` - Boolean value (true/false)

**Note**: String literals must be enclosed in single quotes: `'value'`

### Statements

#### CREATE TABLE

Create a new table with specified columns and data types.

```sql
CREATE TABLE students (id INT, name VARCHAR(20), age INT, active BOOL);
CREATE TABLE courses (student_id INT, course VARCHAR(20), grade INT);
```

#### CREATE INDEX

Create a B-tree index on a table column for faster queries.

```sql
CREATE INDEX students_age_idx ON students (age);
CREATE INDEX courses_grade_idx ON courses (grade);
```

#### INSERT INTO

Insert records into a table. All fields must be provided in the VALUES clause.

```sql
INSERT INTO students (id, name, age, active) VALUES (1, 'Alice', 20, true);
INSERT INTO students (id, name, age, active) VALUES (2, 'Bob', 22, false);
INSERT INTO courses (student_id, course, grade) VALUES (1, 'Math', 95);
INSERT INTO courses (student_id, course, grade) VALUES (1, 'CS', 88);
```

#### SELECT

Query data from tables with support for:

- Field selection
- WHERE clauses with all comparison operators (=, !=, >, <, >=, <=)
- AND/OR logical operators
- ORDER BY for sorting (single or multiple fields)
- GROUP BY with aggregation functions (MAX, MIN, COUNT, SUM, DISTINCT)
- LIMIT and OFFSET clauses for pagination
- Joins (using Cartesian product with WHERE clause)

```sql
-- Basic query
SELECT id, name, age FROM students;

-- WHERE clause with comparison operators
SELECT name FROM students WHERE id = 2;
SELECT id, name, age, active FROM students WHERE age > 20 AND age < 25;
SELECT id, name, age, active FROM students WHERE active = true;

-- Complex WHERE clauses with AND/OR
SELECT id, name, age, active FROM students WHERE (age > 20 OR age < 18) AND active = true;

-- ORDER BY
SELECT name, age FROM students ORDER BY age;
SELECT name, course, grade FROM courses ORDER BY grade, name;

-- GROUP BY with aggregations
SELECT student_id, MAX(grade), MIN(grade) FROM courses GROUP BY student_id;
SELECT student_id, COUNT(grade), SUM(grade) FROM courses GROUP BY student_id;

-- Joins
SELECT name, course, grade FROM students, courses WHERE id = student_id;

-- LIMIT and OFFSET
SELECT name FROM students LIMIT 5;
SELECT name FROM students LIMIT 5 OFFSET 10;
```

#### UPDATE

Modify existing records.

```sql
UPDATE students SET age = 21 WHERE name = 'Alice';
UPDATE students SET active = false WHERE id = 1;
```

#### DELETE

Remove records from a table.

```sql
DELETE FROM students WHERE id = 2;
DELETE FROM students WHERE age < 18;
```

#### EXPLAIN

View the query execution plan.

```sql
EXPLAIN SELECT name, age FROM students WHERE age = 20;
EXPLAIN SELECT student_id, course, grade FROM courses WHERE grade = 85;
```

#### SET

Configure session variables.

```sql
-- Disable materialization for query optimization
SET no_materialize = true;
```

## Tools

### Log Viewer

CraneDB provides a utility to inspect the contents of the log file for debugging and educational purposes.

1. **Build the logviewer**:

   ```bash
   make build-logviewer
   ```

2. **Run the logviewer**:
   ```bash
   ./bin/logviewer ./cranedb_data/cranedb.log
   ```

The output will show all log records along with their LSN, type, transaction ID, and the values before/after modification.

### B+Tree Viewer

CraneDB provides an interactive terminal UI to explore B+Tree index structures. This tool helps visualize the internal structure of B+Tree indexes, including internal nodes, leaf nodes, key ranges, and record identifiers (RIDs).

1. **Build the btreeviewer**:

   ```bash
   make build-btreeviewer
   ```

2. **List available indices**:

   ```bash
   ./bin/btreeviewer -db ./cranedb_data
   ```

3. **Open interactive explorer for a specific index**:

   ```bash
   ./bin/btreeviewer -db ./cranedb_data -index idx_name
   ```

4. **Using the convenience make target** (uses `/tmp/test_db` and `idx_id` by default):
   ```bash
   make btree
   ```

### Data Generator

A utility tool to quickly generate test databases with tables and indices for testing and exploration purposes.

1. **Build the gendata tool**:

   ```bash
   make build-gendata
   ```

2. **Generate test data**:

   ```bash
   ./bin/gendata -db ./test_data -count 1000 -random
   ```

   Options:
   - `-db` - Path to the database directory (default: `./test_data`)
   - `-count` - Number of records to insert (default: `100`)
   - `-random` - Randomize IDs and names (default: `true`)
   - `-table` - Name of the table to create (default: `users`)

3. **Using the convenience make target** (uses `/tmp/test_db` and generates 1000 records by default):
   ```bash
   make gen
   ```

The tool creates:

- A table with `id` (INT) and `name` (VARCHAR(10)) columns
- Two B+Tree indices: `idx_id` on the `id` field and `idx_name` on the `name` field
- The specified number of records with either sequential or random data
