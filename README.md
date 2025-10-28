# CraneDB

A relational database implementation written in Go, built by following the principles and concepts from the "Database Design and Implementation" book.

## About

This project is an educational implementation of a relational database management system (RDBMS) from scratch. The goal is to understand the fundamental concepts and internals of database systems by implementing core components such as:

- File management and storage
- Log management and recovery
- Buffer management
- Transaction management
- Metadata management
- Query processing (planned)
- Indexing and optimization (planned)

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

## Project Structure

```
internal/
├── buffer/         # Buffer pool management
├── file/           # File and page management
├── log/            # Write-ahead logging system
├── metadata/       # Database metadata management
├── record/         # Record storage and schema management
└── transaction/    # Transaction management and concurrency control
```

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
- **Schema Support**: Dynamic schema definition with integer and string field types
- **Record Layout**: Efficient record storage with offset-based field access
- **Table Scanning**: Iterator-based table scanning with insert, update, and delete operations
- **Record Identification**: Unique RID (Record ID) system for record addressing

### Metadata Management
- **Table Management**: Create, drop, and query table metadata
- **View Management**: Virtual table support with view definition storage
- **Index Management**: Index metadata tracking (no actual index data structures implemented)
- **Statistics**: Table statistics collection for cost estimation

### Testing
- **Comprehensive Test Suite**: Unit tests for all major components
- **Concurrency Testing**: Multi-threaded tests for lock management and buffer pool
- **Integration Testing**: End-to-end tests for transaction and recovery scenarios

## Status

✅ **Core Components Complete** - The fundamental database components are implemented and tested:

- ✅ File and page management
- ✅ Buffer pool with concurrency control
- ✅ Write-ahead logging and recovery
- ✅ Transaction management with ACID properties
- ✅ Concurrency control with two-phase locking
- ✅ Record storage and schema management
- ✅ Metadata management for tables, views, and indexes

🚧 **Future Development** - Major components remaining to be implemented:

- **Query Processing, Parsing & Planning**: SQL parser, relational algebra, cost estimation, and execution plan generation
- **Indexing**: B-tree indexes, hash indexes, and index-aware query operations
- **Materialization & Sorting**: Temporary tables, external sorting, grouping, and merge joins
- **Buffer Optimization**: Multibuffer algorithms for sorting and joins
- **Query Optimization**: Equivalent query trees, cost-based optimization, and plan selection

**Note**: This project is not intended for production use and serves as an educational implementation of database internals.
