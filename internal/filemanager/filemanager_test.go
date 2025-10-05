package filemanager

import (
	"os"
	"testing"
)

func TestFileMgr(t *testing.T) {
	// Create a temporary directory for our test database
	tempDir, err := os.MkdirTemp("", "filemanager_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	// Initialize FileMgr with 400-byte blocks
	blockSize := 400
	fm := NewFileMgr(tempDir, blockSize)
	defer fm.Close()

	// Test basic operations on a single file
	filename := "test.db"

	// 1. Append a new block (should be block 0)
	blk0 := fm.Append(filename)
	if blk0.Number() != 0 {
		t.Errorf("First block should be 0, got %d", blk0.Number())
	}

	// 2. Write some data to block 0
	page := NewPage(blockSize)
	data := "Hello, World!"
	page.SetString(0, data)
	fm.Write(blk0, page)

	// 3. Read back the data from block 0
	readPage := NewPage(blockSize)
	fm.Read(blk0, readPage)
	readData := readPage.GetString(0)
	if readData != data {
		t.Errorf("Expected to read %q, got %q", data, readData)
	}

	// 4. Append another block (should be block 1)
	blk1 := fm.Append(filename)
	if blk1.Number() != 1 {
		t.Errorf("Second block should be 1, got %d", blk1.Number())
	}

	// 5. Write different data to block 1
	data2 := "Second block data"
	page.SetString(0, data2)
	fm.Write(blk1, page)

	// 6. Read back both blocks to verify they maintain separate data
	fm.Read(blk0, readPage)
	if readPage.GetString(0) != data {
		t.Errorf("Block 0 data changed, expected %q, got %q", data, readPage.GetString(0))
	}

	fm.Read(blk1, readPage)
	if readPage.GetString(0) != data2 {
		t.Errorf("Block 1 data wrong, expected %q, got %q", data2, readPage.GetString(0))
	}
}
