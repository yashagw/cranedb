.PHONY: help build build-server build-client clean test run-server run-client

# Default target
help:
	@echo "CraneDB - A Simple Relational Database"
	@echo ""
	@echo "Available targets:"
	@echo "  make build         - Build both server and client"
	@echo "  make build-server  - Build server binary"
	@echo "  make build-client  - Build client binary"
	@echo "  make clean         - Remove built binaries and database files"
	@echo "  make test          - Run all tests"
	@echo "  make run-server    - Start the database server"
	@echo "  make run-client    - Start the database client"

# Build targets
build: build-server build-client
	@echo "✓ Build complete"

build-server:
	@echo "Building server..."
	@mkdir -p bin
	@go build -o bin/server ./cmd/server
	@echo "✓ Server built: bin/server"

build-client:
	@echo "Building client..."
	@mkdir -p bin
	@go build -o bin/client ./cmd/client
	@echo "✓ Client built: bin/client"

# Clean target
clean:
	@echo "Cleaning up..."
	@rm -rf bin/
	@rm -rf cranedb_data/*.tbl cranedb_data/*.log
	@echo "✓ Clean complete"

# Test targets
test:
	@echo "Running tests..."
	@go test ./...

# Run targets
run-server: build-server
	@echo "Starting CraneDB server on port 8080..."
	@./bin/server

run-client: build-client
	@echo "Starting CraneDB client..."
	@./bin/client
