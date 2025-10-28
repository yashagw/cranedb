package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestManager(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	blockSize := 400
	fm, err := NewManager(tempDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()

	filename := "test.db"

	// Test 1: Append a new block (should be block 0)
	blk0, err := fm.Append(filename)
	assert.NoError(t, err)
	assert.Equal(t, 0, blk0.Number(), "First block should be 0")

	page := NewPage(blockSize)
	data := "Hello, World!"
	page.SetString(0, data)
	err = fm.Write(blk0, page)
	assert.NoError(t, err)

	readPage := NewPage(blockSize)
	err = fm.Read(blk0, readPage)
	assert.NoError(t, err)
	assert.Equal(t, data, readPage.GetString(0), "Expected to read %q, got %q", data, readPage.GetString(0))

	// Test 2: Append another block (should be block 1)
	blk1, err := fm.Append(filename)
	assert.NoError(t, err)
	assert.Equal(t, 1, blk1.Number(), "Second block should be 1")

	data2 := "Second block data"
	page.SetString(0, data2)
	err = fm.Write(blk1, page)
	assert.NoError(t, err)

	// Test 3: Read back both blocks to verify they maintain separate data
	err = fm.Read(blk0, readPage)
	assert.NoError(t, err)
	assert.Equal(t, data, readPage.GetString(0), "Block 0 data should be the same")

	err = fm.Read(blk1, readPage)
	assert.NoError(t, err)
	assert.Equal(t, data2, readPage.GetString(0), "Block 1 data should be the same")
}

func TestTotalBlocks(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	blockSize := 400
	fm, err := NewManager(tempDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()

	// Test 1: Append 5 blocks and check TotalBlocks
	filename1 := "test1.db"
	for i := 0; i < 5; i++ {
		blk, err := fm.Append(filename1)
		assert.NoError(t, err)
		assert.Equal(t, i, blk.Number())
	}

	numBlocks, err := fm.GetTotalBlocks(filename1)
	assert.NoError(t, err)
	assert.Equal(t, 5, numBlocks, "File should have 5 blocks")

	// Test 2: Write directly to 5th block of new file
	filename2 := "test2.db"
	page := NewPage(blockSize)
	page.SetString(0, "Fifth block data")
	blk4 := NewBlockID(filename2, 4)

	err = fm.Write(blk4, page)
	assert.NoError(t, err)

	numBlocks, err = fm.GetTotalBlocks(filename2)
	assert.NoError(t, err)
	assert.Equal(t, 5, numBlocks, "Writing to block 4 should make file have 5 blocks")

	// Test 3: For Empty/New File
	filename3 := "test3.db"
	numBlocks, err = fm.GetTotalBlocks(filename3)
	assert.NoError(t, err)
	assert.Equal(t, 0, numBlocks, "New file should have 0 blocks")
}
