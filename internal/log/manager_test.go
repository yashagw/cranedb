package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yashagw/cranedb/internal/file"
)

func TestManager(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	fm, err := file.NewManager(tempDir, 32)
	assert.NoError(t, err)

	logFile := "test.log"
	lm, err := NewManager(fm, logFile)
	assert.NoError(t, err)

	boundary := lm.logPage.GetInt(0)
	assert.Equal(t, boundary, 32)

	logSize, err := fm.GetTotalBlocks(logFile)
	assert.NoError(t, err)
	assert.Equal(t, logSize, 1)

	// Test when log file already exists
	fm.Write(file.NewBlockID(logFile, 1), file.NewPage(fm.BlockSize()))

	_, err = NewManager(fm, logFile)
	assert.NoError(t, err)

	logSize, err = fm.GetTotalBlocks(logFile)
	assert.NoError(t, err)
	assert.Equal(t, logSize, 2)
}

func TestLog(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	fm, err := file.NewManager(tempDir, 32)
	assert.NoError(t, err)

	logFile := "test.log"
	lm, err := NewManager(fm, logFile)
	assert.NoError(t, err)

	tests := []struct {
		name             string
		data             []byte
		expectedLogSize  int
		expectedboundary int
		expectedLSN      int
	}{
		{
			name:             "test loging first record",
			data:             []byte("test record"),
			expectedLogSize:  1,
			expectedboundary: 17, // 32 (boundary before the write) - 15 (4 bytes for length and 11 bytes for data)
			expectedLSN:      1,
		},
		{
			name:             "test logging second record to be flushed to the same first block",
			data:             []byte("record 2"),
			expectedLogSize:  1,
			expectedboundary: 5, // 17 (boundary before the write) - 12 (4 bytes for length and 8 bytes for data)
			expectedLSN:      2,
		},
		{
			name:             "test logging third record to be flushed to the new second block",
			data:             []byte("record 3"),
			expectedLogSize:  2,
			expectedboundary: 20, // 32 (new block - boundary before the write) - 12 (4 bytes for length and 8 bytes for data)
			expectedLSN:      3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lsn, err := lm.Append(tt.data)
			assert.NoError(t, err)
			assert.Equal(t, lsn, tt.expectedLSN)

			boundary := lm.logPage.GetInt(0)
			assert.Equal(t, boundary, tt.expectedboundary)

			logSize, err := fm.GetTotalBlocks(logFile)
			assert.NoError(t, err)
			assert.Equal(t, logSize, tt.expectedLogSize)
		})
	}
}

func TestIterator(t *testing.T) {
	tempDir := t.TempDir()
	defer os.RemoveAll(tempDir)

	fm, err := file.NewManager(tempDir, 32)
	assert.NoError(t, err)

	logFile := "test.log"
	lm, err := NewManager(fm, logFile)
	assert.NoError(t, err)

	records := [][]byte{
		[]byte("record one"),
		[]byte("record two"),
		[]byte("record three"),
		[]byte("record four"),
		[]byte("record five"),
		[]byte("record six"),
		[]byte("record seven"),
		[]byte("record eight"),
		[]byte("record nine"),
		[]byte("record ten"),
		[]byte("record eleven"),
		[]byte("record twelve"),
		[]byte("record thirteen"),
	}
	for _, record := range records {
		lm.Append(record)
	}

	iter, err := lm.Iterator()
	assert.NoError(t, err)

	for i := 12; i >= 0; i-- {
		assert.True(t, iter.HasNext())
		rec := iter.Next()
		assert.Equal(t, string(rec), string(records[i]))
	}
	assert.False(t, iter.HasNext())
}
