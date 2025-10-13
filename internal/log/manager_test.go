package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yashagw/cranedb/internal/file"
)

func TestNewLogMgr(t *testing.T) {
	dataDir := "testdata"
	logFile := "testlogfile"

	fileManager := file.NewManager(dataDir, 32)
	t.Cleanup(func() {
		fileManager.Close()
		os.Remove(filepath.Join(dataDir, logFile))
	})

	logManager := NewManager(fileManager, logFile)

	boundary := logManager.logpage.GetInt(0)
	if boundary != 32 {
		t.Errorf("boundary = %d, want %d", boundary, 32)
	}

	logSize, err := fileManager.GetNumBlocks(logFile)
	if err != nil {
		t.Fatalf("GetNumBlocks failed: %v", err)
	}
	if logSize != 1 {
		t.Errorf("logSize = %d, want %d", logSize, 1)
	}

	// Test when log file already exists
	fileManager.Write(file.NewBlockID(logFile, 1), file.NewPage(fileManager.BlockSize()))

	_ = NewManager(fileManager, logFile)

	logSize, err = fileManager.GetNumBlocks(logFile)
	if err != nil {
		t.Fatalf("GetNumBlocks failed: %v", err)
	}
	if logSize != 2 {
		t.Errorf("logSize = %d, want %d", logSize, 2)
	}
}

func TestLog(t *testing.T) {
	dataDir := "testdata"
	logFile := "testlogfile1"

	fileManager := file.NewManager(dataDir, 32)
	t.Cleanup(func() {
		fileManager.Close()
		os.Remove(filepath.Join(dataDir, logFile))
	})

	logManager := NewManager(fileManager, logFile)

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
			lsn := logManager.Append(tt.data)
			if lsn != tt.expectedLSN {
				t.Errorf("lsn = %d, want %d", lsn, tt.expectedLSN)
			}

			boundary := logManager.logpage.GetInt(0)
			if boundary != tt.expectedboundary {
				t.Errorf("boundary = %d, want %d", boundary, tt.expectedboundary)
			}

			logSize, err := fileManager.GetNumBlocks(logFile)
			if err != nil {
				t.Fatalf("GetNumBlocks failed: %v", err)
			}
			if logSize != tt.expectedLogSize {
				t.Errorf("logSize = %d, want %d", logSize, tt.expectedLogSize)
			}
		})
	}
}

func TestIterator(t *testing.T) {
	dataDir := "testdata"
	logFile := "testlogfile2"

	fileManager := file.NewManager(dataDir, 32)
	t.Cleanup(func() {
		fileManager.Close()
		os.Remove(filepath.Join(dataDir, logFile))
	})

	logManager := NewManager(fileManager, logFile)

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
		logManager.Append(record)
	}

	iter := logManager.Iterator()

	for i := 12; i >= 0; i-- {
		if !iter.HasNext() {
			t.Fatalf("HasNext returned false, want true")
		}

		rec := iter.Next()
		if string(rec) != string(records[i]) {
			t.Errorf("record data, got = %s, want %s", rec, records[i])
		}
	}

	// After reading all records, HasNext should return false
	if iter.HasNext() {
		t.Errorf("HasNext returned true after reading all records, want false")
	}
}
