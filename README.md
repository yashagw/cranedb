# CraneDB

A relational database implementation written in Go, built by following the principles and concepts from the "Database Design and Implementation" book.

## About

This project is an educational implementation of a relational database management system (RDBMS) from scratch. The goal is to understand the fundamental concepts and internals of database systems by implementing core components such as:

- File management and storage
- Log management and recovery
- Buffer management
- Transaction management
- Metadata management
- Query processing
- Indexing

## Current Implementation

The database has implemented the following core components:

- **File Manager**: Handles low-level file operations, page management, and block allocation
- **Log Manager**: Manages write-ahead logging for transaction recovery and durability
- **Buffer Manager**: Implements buffer pool with pin/unpin mechanism and LRU-style management
- **Transaction Manager**: Provides ACID transaction support with concurrency control
- **Recovery Manager**: Implements undo-only recovery algorithm for crash recovery
- **Concurrency Manager**: Manages shared and exclusive locks with deadlock prevention
- **Record Manager**: Handles record storage, schema management, and table scanning
- **Metadata Manager**: Manages database metadata including tables, views, indexes, and statistics
- **Parser**: SQL parser and lexer for parsing SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, CREATE VIEW, and CREATE INDEX statements with support for ORDER BY and GROUP BY clauses
- **Query Planner**: Generates execution plans for SELECT queries with support for joins, predicates, projections, sorting, grouping, aggregation functions, and index-aware optimization
- **Update Planner**: Executes INSERT, UPDATE, DELETE, CREATE TABLE, CREATE VIEW, and CREATE INDEX statements

## Features

### Core Storage Engine
- **File Management**: Fixed-size block storage with efficient read/write operations
- **Page Management**: In-memory page abstraction with binary data serialization
- **Buffer Pool**: LRU-style buffer management with pin/unpin mechanism and timeout handling

### Transaction Management
- **ACID Properties**: Full transaction support with atomicity, consistency, isolation, and durability
- **Concurrency Control**: Two-phase locking with shared and exclusive locks
- **Deadlock Prevention**: Timeout-based lock management to prevent indefinite waiting
- **Recovery**: Undo-only recovery algorithm for crash recovery and transaction rollback

### Logging and Recovery
- **Write-Ahead Logging**: All changes logged before being written to disk
- **Log Records**: Support for checkpoint, start, commit, rollback, and data modification records
- **Crash Recovery**: Automatic recovery from system crashes using log replay
- **Log Iteration**: Efficient forward and backward iteration through log records

### Record Management
- **Schema Support**: Dynamic schema definition with integer, boolean, and string field types
- **Record Layout**: Efficient record storage with offset-based field access
- **Table Scanning**: Iterator-based table scanning with insert, update, and delete operations
- **Record Identification**: Unique RID (Record ID) system for record addressing

### Metadata Management
- **Table Management**: Create, drop, and query table metadata
- **View Management**: Virtual table support with view definition storage
- **Index Management**: Index metadata tracking and index creation
- **Statistics**: Table statistics collection for cost estimation

### Query Processing
- **SQL Parser**: Lexical analysis and parsing of SQL statements
- **Query Planning**: Execution plan generation with cost estimation and index-aware optimization
- **Relational Algebra**: Support for product (join), select (filter), project (field selection), sort, group, and materialization operations
- **Query Execution**: Iterator-based query execution with lazy evaluation
- **Expression Evaluation**: Support for field references and constant values in expressions
- **Predicate Evaluation**: WHERE clause filtering with support for comparison operators (=, !=, >, <, >=, <=) and AND/OR conditions
- **Sorting**: ORDER BY clause support for sorting query results by one or more fields
- **Grouping**: GROUP BY clause support for grouping records by specified fields
- **Aggregation Functions**: Support for MAX, MIN, COUNT and SUM aggregation functions in GROUP BY queries
- **Update Operations**: Execution of INSERT, UPDATE, and DELETE statements with predicate support and automatic index maintenance
- **Materialization**: Temporary table materialization for query optimization, especially beneficial for nested loop joins

## Status

✅ **Core Components Complete**
- ✅ File and page management
- ✅ Buffer pool with concurrency control
- ✅ Write-ahead logging and recovery
- ✅ Transaction management with ACID properties
- ✅ Concurrency control with two-phase locking
- ✅ Record storage and schema management (integer, boolean, and string field types)
- ✅ Metadata management for tables, views, and indexes
- ✅ SQL parsing and lexing
- ✅ Query planning and execution
- ✅ Relational algebra operations (product, select, project, sort, group, materialize)
- ✅ Client-server architecture
- ✅ B-tree indexes
- ✅ Materialization for query optimization
- ✅ Sorting (ORDER BY) support
- ✅ Grouping (GROUP BY) with MAX, MIN, COUNT and SUM aggregation functions
- ✅ Comparison operators (=, !=, >, <, >=, <=) and AND/OR conditions in WHERE clauses

---

**Note**: This project is not intended for production use and serves as an educational implementation of database internals.
