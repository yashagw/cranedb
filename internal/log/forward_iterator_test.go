package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/yashagw/cranedb/internal/file"
)

func TestForwardIterator(t *testing.T) {
	tempDir := t.TempDir()

	fm, err := file.NewManager(tempDir, 32)
	assert.NoError(t, err)

	logFile := "test_forward.log"
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
	}
	for _, rec := range records {
		lsn := lm.GetNextLatestLSN()
		err := lm.Append(rec, lsn)
		assert.NoError(t, err)
	}

	err = lm.ForceFlush()
	assert.NoError(t, err)

	fi, err := NewForwardIterator(fm, logFile, 0)
	assert.NoError(t, err)

	// Read all records and verify forward order
	var got []string
	for fi.HasNext() {
		rec := fi.Next()
		if rec == nil {
			break
		}
		got = append(got, string(rec))
	}

	assert.Equal(t, len(records), len(got))
	for i, rec := range records {
		assert.Equal(t, string(rec), got[i])
	}
}

func TestForwardIteratorRefresh(t *testing.T) {
	tempDir := t.TempDir()

	fm, err := file.NewManager(tempDir, 32)
	assert.NoError(t, err)

	logFile := "test_refresh.log"
	lm, err := NewManager(fm, logFile)
	assert.NoError(t, err)

	// Write initial records
	initialRecords := [][]byte{
		[]byte("first"),
		[]byte("second"),
	}
	for _, rec := range initialRecords {
		lsn := lm.GetNextLatestLSN()
		err := lm.Append(rec, lsn)
		assert.NoError(t, err)
	}
	err = lm.ForceFlush()
	assert.NoError(t, err)

	fi, err := NewForwardIterator(fm, logFile, 0)
	assert.NoError(t, err)

	// Read initial records
	var got []string
	for fi.HasNext() {
		rec := fi.Next()
		if rec == nil {
			break
		}
		got = append(got, string(rec))
	}
	assert.Equal(t, 2, len(got))

	// Write more records
	newRecords := [][]byte{
		[]byte("third"),
		[]byte("fourth"),
	}
	for _, rec := range newRecords {
		lsn := lm.GetNextLatestLSN()
		err := lm.Append(rec, lsn)
		assert.NoError(t, err)
	}
	err = lm.ForceFlush()
	assert.NoError(t, err)

	// Refresh should detect new records
	hasNew, err := fi.Refresh()
	assert.NoError(t, err)
	assert.True(t, hasNew)

	// Read new records
	for fi.HasNext() {
		rec := fi.Next()
		if rec == nil {
			break
		}
		got = append(got, string(rec))
	}

	// Should have all 4 records in order
	assert.Equal(t, 4, len(got))
	assert.Equal(t, "first", got[0])
	assert.Equal(t, "second", got[1])
	assert.Equal(t, "third", got[2])
	assert.Equal(t, "fourth", got[3])
}
