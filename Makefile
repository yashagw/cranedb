.PHONY: help build build-server build-client build-logviewer clean test run-server run-client

# Default target
help:
	@echo "CraneDB - A Simple Relational Database"
	@echo ""
	@echo "Available targets:"
	@echo "  make build         - Build both server and client"
	@echo "  make build-server  - Build server binary"
	@echo "  make build-client  - Build client binary"
	@echo "  make build-logviewer - Build logviewer binary"
	@echo "  make clean         - Remove built binaries and database files"
	@echo "  make test          - Run all tests"
	@echo "  make run-server    - Start the database server"
	@echo "  make run-client    - Start the database client"

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
	@./bin/server

run-client: build-client
	@./bin/client
