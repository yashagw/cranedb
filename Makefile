.PHONY: help build build-server build-client build-logviewer build-btreeviewer build-gendata build-tools clean test run-server run-client

# Default target
help:
	@echo "CraneDB - A Simple Relational Database"
	@echo ""
	@echo "Available targets:"
	@echo "  make build         - Build both server and client"
	@echo "  make build-server  - Build server binary"
	@echo "  make build-client  - Build client binary"
	@echo "  make build-logviewer - Build logviewer binary"
	@echo "  make build-btreeviewer - Build B+Tree index explorer"
	@echo "  make build-gendata   - Build data generation tool"
	@echo "  make build-tools    - Build both btreeviewer and gendata"
	@echo "  make clean         - Remove built binaries and database files"
	@echo "  make test          - Run all tests"
	@echo "  make run-server    - Start the database server"
	@echo "  make run-client    - Start the database client"
	@echo "  make gen           - Generate test B+Tree data (/tmp/btree_test_db)"
	@echo "  make btree         - Start B+Tree explorer (/tmp/btree_test_db)"

# Build targets
build: build-server build-client
	@echo "✓ Build complete"

build-server:
	@echo "Building server (release)..."
	@mkdir -p bin
	@go build -tags release -o bin/server ./cmd/server
	@echo "✓ Server built: bin/server"

build-client:
	@echo "Building client (release)..."
	@mkdir -p bin
	@go build -tags release -o bin/client ./cmd/client
	@echo "✓ Client built: bin/client"

build-logviewer:
	@echo "Building logviewer (release)..."
	@mkdir -p bin
	@go build -tags release -o bin/logviewer ./cmd/logviewer
	@echo "✓ Logviewer built: bin/logviewer"

build-debug:
	@echo "Building server (debug)..."
	@mkdir -p bin
	@go build -o bin/server_debug ./cmd/server
	@echo "✓ Debug Server built: bin/server_debug"

build-btreeviewer:
	@echo "Building btreeviewer..."
	@mkdir -p bin
	@go build -o bin/btreeviewer ./cmd/btreeviewer/*.go
	@echo "✓ B+Tree Viewer built: bin/btreeviewer"

build-gendata:
	@echo "Building gendata..."
	@mkdir -p bin
	@go build -o bin/gendata ./cmd/gendata/*.go
	@echo "✓ Data Generator built: bin/gendata"

# Clean target
clean:
	@echo "Cleaning up..."
	@rm -rf bin/
	@rm -rf cranedb_data2/**
	@rm -rf cranedb_data/**
	@echo "✓ Clean complete"

# Test targets
test:
	@echo "Running tests..."
	@go test ./...

# Run targets
run-server: build-server
	PORT=8080 DB_DIR=/tmp/test_db ./bin/server

run-client: build-client
	CRANEDB_PORT=8080 ./bin/client

BTREE_TEST_DB = /tmp/test_db
COUNT ?= 1000
INDEX ?= idx_id

gen: build-gendata
	@rm -rf $(BTREE_TEST_DB)
	@./bin/gendata -db $(BTREE_TEST_DB) -count $(COUNT) -random

btree: build-btreeviewer
	@./bin/btreeviewer -db $(BTREE_TEST_DB) -index $(INDEX)
