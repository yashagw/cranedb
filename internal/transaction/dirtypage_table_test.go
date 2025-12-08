package transaction

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/file"
)

func TestDirtyPageTable_BasicOperations(t *testing.T) {
	// Test NewDirtyPageTable
	dpt := NewDirtyPageTable()
	require.NotNil(t, dpt)
	all := dpt.GetAll()
	assert.Empty(t, all)

	// Test Add - add new page
	block1 := file.NewBlockID("test.db", 0)
	dpt.Add(block1, 100)
	entry, exists := dpt.Get(block1)
	require.True(t, exists)
	assert.Equal(t, int64(100), entry.RecLSN)

	// Test Add - add multiple pages
	block2 := file.NewBlockID("test.db", 1)
	block3 := file.NewBlockID("other.db", 0)
	dpt.Add(block2, 200)
	dpt.Add(block3, 300)
	entry2, exists2 := dpt.Get(block2)
	require.True(t, exists2)
	assert.Equal(t, int64(200), entry2.RecLSN)
	entry3, exists3 := dpt.Get(block3)
	require.True(t, exists3)
	assert.Equal(t, int64(300), entry3.RecLSN)

	// Test Add - does not overwrite existing entry (recLSN is set once)
	dpt.Add(block1, 150) // Try to add with different LSN
	entry, exists = dpt.Get(block1)
	require.True(t, exists)
	assert.Equal(t, int64(100), entry.RecLSN) // Should still be 100, not 150

	// Test Get - get non-existent page
	block4 := file.NewBlockID("nonexistent.db", 0)
	_, exists = dpt.Get(block4)
	assert.False(t, exists)

	// Test Get - verify returns a copy to prevent external modification
	entry.RecLSN = 999               // Modify the returned entry
	entry2, exists = dpt.Get(block1) // Get again
	require.True(t, exists)
	assert.Equal(t, int64(100), entry2.RecLSN) // Original value should be unchanged

	// Test GetAll - on empty table
	dpt2 := NewDirtyPageTable()
	all = dpt2.GetAll()
	assert.NotNil(t, all)
	assert.Empty(t, all)

	// Test GetAll - with multiple pages
	all = dpt.GetAll()
	require.Len(t, all, 3)

	key1 := file.MakeBlockKey(block1)
	key2 := file.MakeBlockKey(block2)
	key3 := file.MakeBlockKey(block3)

	entry1, exists := all[key1]
	require.True(t, exists)
	assert.Equal(t, int64(100), entry1.RecLSN)

	entry2, exists = all[key2]
	require.True(t, exists)
	assert.Equal(t, int64(200), entry2.RecLSN)

	entry3, exists = all[key3]
	require.True(t, exists)
	assert.Equal(t, int64(300), entry3.RecLSN)

	// Test GetAll - verify returns a snapshot (immutability)
	entry1.RecLSN = 999             // Modify entry in snapshot
	entry, exists = dpt.Get(block1) // Get from table
	require.True(t, exists)
	assert.Equal(t, int64(100), entry.RecLSN) // Original value should be unchanged

	// Test GetAll - verify returns a new snapshot each time
	block4 = file.NewBlockID("new.db", 0)
	dpt.Add(block4, 400)
	all2 := dpt.GetAll()
	assert.Len(t, all2, 4)
	assert.Len(t, all, 3) // Original snapshot should be unchanged

	// Test Remove - remove existing page
	err := dpt.Remove(block2)
	require.NoError(t, err)
	_, exists = dpt.Get(block2)
	assert.False(t, exists)

	// Test Remove - remove non-existent page
	err = dpt.Remove(block4)
	require.NoError(t, err)  // First remove succeeds
	err = dpt.Remove(block4) // Second remove should fail
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found in dirty page table")

	// Test Remove - remove multiple pages
	block5 := file.NewBlockID("test.db", 2)
	block6 := file.NewBlockID("test.db", 3)
	dpt.Add(block5, 500)
	dpt.Add(block6, 600)
	err = dpt.Remove(block5)
	require.NoError(t, err)
	_, exists = dpt.Get(block5)
	assert.False(t, exists)
	_, exists = dpt.Get(block6)
	assert.True(t, exists)

	// Test file.BlockID equality
	block1Copy := file.NewBlockID("test.db", 0)
	entry, exists = dpt.Get(block1Copy)
	require.True(t, exists)
	assert.Equal(t, int64(100), entry.RecLSN) // Should find same entry
}

func TestDirtyPageTable_ConcurrentAccess(t *testing.T) {
	dpt := NewDirtyPageTable()
	const numGoroutines = 10
	const numPages = 100

	var wg sync.WaitGroup

	// Test concurrent Add operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numPages; j++ {
				block := file.NewBlockID("test.db", id*numPages+j)
				lsn := int64(id*numPages + j)
				dpt.Add(block, lsn)
			}
		}(i)
	}
	wg.Wait()

	// Verify all pages were added
	all := dpt.GetAll()
	assert.Len(t, all, numGoroutines*numPages)

	// Test concurrent Get operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numPages; j++ {
				block := file.NewBlockID("test.db", id*numPages+j)
				entry, exists := dpt.Get(block)
				require.True(t, exists)
				expectedLSN := int64(id*numPages + j)
				assert.Equal(t, expectedLSN, entry.RecLSN)
			}
		}(i)
	}
	wg.Wait()

	// Test concurrent GetAll operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			snapshot := dpt.GetAll()
			assert.Len(t, snapshot, numGoroutines*numPages)
		}()
	}
	wg.Wait()

	// Test concurrent Add and Get operations
	wg.Add(numGoroutines * 2)
	for i := 0; i < numGoroutines; i++ {
		// Writers
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numPages; j++ {
				block := file.NewBlockID("concurrent.db", id*numPages+j)
				lsn := int64(id*numPages + j + 1000)
				dpt.Add(block, lsn)
			}
		}(i)
		// Readers
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numPages; j++ {
				block := file.NewBlockID("test.db", id*numPages+j)
				_, exists := dpt.Get(block)
				assert.True(t, exists)
			}
		}(i)
	}
	wg.Wait()

	// Test concurrent Remove operations
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numPages; j++ {
				block := file.NewBlockID("test.db", id*numPages+j)
				err := dpt.Remove(block)
				// Some removes may fail if already removed, that's okay
				_ = err
			}
		}(i)
	}
	wg.Wait()

	// Verify pages from test.db were removed, but concurrent.db pages remain
	all = dpt.GetAll()
	// Should have concurrent.db pages (1000) but not test.db pages
	assert.Equal(t, numGoroutines*numPages, len(all)) // concurrent.db pages remain

	// Test concurrent Add, Get, and Remove operations
	wg.Add(numGoroutines * 3)
	for i := 0; i < numGoroutines; i++ {
		// Adders
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				block := file.NewBlockID("mixed.db", id*50+j)
				dpt.Add(block, int64(id*50+j))
			}
		}(i)
		// Getters
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				block := file.NewBlockID("mixed.db", id*50+j)
				_, _ = dpt.Get(block)
			}
		}(i)
		// Removers
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				block := file.NewBlockID("mixed.db", id*50+j)
				_ = dpt.Remove(block)
			}
		}(i)
	}
	wg.Wait()

	// Final verification - table should still be in a consistent state
	all = dpt.GetAll()
	// Verify we can still read all entries without panicking
	for key, entry := range all {
		assert.NotNil(t, entry)
		assert.GreaterOrEqual(t, entry.RecLSN, int64(0))
		_ = key // Use key to avoid unused variable
	}
}
