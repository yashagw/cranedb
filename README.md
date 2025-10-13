# CraneDB

A relational database implementation written in Go, built by following the principles and concepts from the "Database Design and Implementation" book.

## About

This project is an educational implementation of a relational database management system (RDBMS) from scratch. The goal is to understand the fundamental concepts and internals of database systems by implementing core components such as:

- File management and storage
- Log management and recovery
- Buffer management
- Query processing
- Transaction management
- Indexing and optimization

## Current Implementation

The database is currently under development with the following components:

- **File Manager**: Handles low-level file operations, page management, and block allocation
- **Log Manager**: Manages write-ahead logging for transaction recovery and durability

## Project Structure

```
internal/
├── filemanager/     # File and page management
└── log/      # Write-ahead logging system
```

## Status

🚧 **Work in Progress** - This is an educational project and is not intended for production use.
