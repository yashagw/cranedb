# CraneDB

A relational database implementation written in Go, built by following the principles and concepts from the "Database Design and Implementation" book.

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
- **Buffer Pool**: LRU-style buffer management with pin/unpin mechanism and timeout handling

### Transaction Management
- **ACID Properties**: Transaction support with atomicity (commit/rollback), durability (write-ahead logging), basic isolation (two-phase locking), and transaction-level consistency
- **Concurrency Control**: Two-phase locking with shared and exclusive locks
- **Deadlock Prevention**: Timeout-based lock management to prevent indefinite waiting
- **Recovery**: Undo-only recovery algorithm for crash recovery and transaction rollback

### Logging and Recovery
- **Write-Ahead Logging**: All changes logged before being written to disk
- **Log Records**: Support for checkpoint, start, commit, rollback, and data modification records
- **Crash Recovery**: Automatic recovery from system crashes using log replay

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
- **Aggregation Functions**: Support for MAX, MIN, COUNT, and SUM aggregation functions in GROUP BY queries
- **Update Operations**: Execution of INSERT, UPDATE, and DELETE statements with predicate support and automatic index maintenance
- **Materialization**: Temporary table materialization for query optimization, especially beneficial for nested loop joins
- **B-tree Indexes**: Efficient B-tree index implementation for fast lookups and range queries
- **EXPLAIN**: Query plan visualization to understand execution strategies

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
- GROUP BY with aggregation functions (MAX, MIN, COUNT, SUM)
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
