package metadata

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestViewManager_BasicOperations(t *testing.T) {
	dbDir := "testdata"
	blockSize := 400

	fm := file.NewManager(dbDir, blockSize)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm := log.NewManager(fm, "testlog")
	defer lm.Close()

	bm := buffer.NewManager(fm, lm, 10)
	lockTable := transaction.NewLockTable()

	// Test 1: Create new ViewManager with new database
	tx1 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm := NewTableManager(true, tx1)
	vm := NewViewManager(true, tm, tx1)
	require.NotNil(t, vm)
	assert.NotNil(t, vm.tableManager)
	tx1.Commit()

	// Test 2: Create ViewManager for existing database
	tx2 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm2 := NewTableManager(false, tx2)
	vm2 := NewViewManager(false, tm2, tx2)
	require.NotNil(t, vm2)
	assert.NotNil(t, vm2.tableManager)
	tx2.Commit()

	// Test 3: Create a new view
	tx3 := transaction.NewTransaction(fm, lm, bm, lockTable)
	viewName := "user_emails"
	viewDef := "SELECT name, email FROM users WHERE active = 1"
	err := vm.CreateView(viewName, viewDef, tx3)
	require.NoError(t, err, "Should create view successfully")
	tx3.Commit()

	// Test 4: Retrieve view definition and verify it matches
	tx4 := transaction.NewTransaction(fm, lm, bm, lockTable)
	retrievedViewDef, err := vm.GetViewDef(viewName, tx4)
	require.NoError(t, err, "Should retrieve view definition successfully")
	assert.Equal(t, viewDef, retrievedViewDef, "Retrieved view definition should match original")
	tx4.Commit()

	// Test 5: Try to get definition for non-existent view
	tx5 := transaction.NewTransaction(fm, lm, bm, lockTable)
	nonExistentViewDef, err := vm.GetViewDef("nonexistent", tx5)
	require.NoError(t, err, "Should not return error for non-existent view")
	assert.Empty(t, nonExistentViewDef, "Should return empty string for non-existent view")
	tx5.Commit()

	// Test 6: Create multiple views with different definitions
	tx6 := transaction.NewTransaction(fm, lm, bm, lockTable)

	views := map[string]string{
		"active_users":   "SELECT * FROM users WHERE active = 1",
		"user_summary":   "SELECT id, name FROM users",
		"product_prices": "SELECT product_id, price FROM products WHERE price > 0",
	}

	for vName, vDef := range views {
		err := vm.CreateView(vName, vDef, tx6)
		require.NoError(t, err, "Should create view %s successfully", vName)
	}
	tx6.Commit()

	// Test 7: Verify all views exist and have correct definitions
	tx7 := transaction.NewTransaction(fm, lm, bm, lockTable)

	// Check original view still exists
	originalViewDef, err := vm.GetViewDef(viewName, tx7)
	require.NoError(t, err)
	assert.Equal(t, viewDef, originalViewDef, "Original view should still exist")

	// Check all new views
	for vName, expectedDef := range views {
		retrievedDef, err := vm.GetViewDef(vName, tx7)
		require.NoError(t, err, "Should retrieve view %s successfully", vName)
		assert.Equal(t, expectedDef, retrievedDef, "View %s definition should match", vName)
	}
	tx7.Commit()

	// Test 8: Verify view catalog data by directly scanning the catalog table
	tx8 := transaction.NewTransaction(fm, lm, bm, lockTable)

	layout, err := tm.GetLayout(ViewCatalogName, tx8)
	require.NoError(t, err, "Should get view catalog layout")

	ts := record.NewTableScan(tx8, layout, ViewCatalogName)
	defer ts.Close()

	foundViews := make(map[string]string)
	for ts.Next() {
		vName := ts.GetString("viewname")
		vDef := ts.GetString("viewdef")
		foundViews[vName] = vDef
	}

	// Verify all created views are in the catalog
	assert.Equal(t, viewDef, foundViews[viewName], "Original view should be in catalog")
	for vName, expectedDef := range views {
		assert.Equal(t, expectedDef, foundViews[vName], "View %s should be in catalog with correct definition", vName)
	}

	// Verify total count
	expectedCount := 1 + len(views) // original view + new views
	assert.Len(t, foundViews, expectedCount, "Should have correct number of views in catalog")

	tx8.Commit()

	// Test 9: Test view definition length limits
	tx9 := transaction.NewTransaction(fm, lm, bm, lockTable)

	// Create a view with maximum allowed definition length
	longViewName := "long_view"
	longViewDef := "SELECT * FROM users WHERE " + "very_long_condition = 1 AND another_condition = 2 AND more_stuff = 3"
	if len(longViewDef) <= MaxViewDef {
		err := vm.CreateView(longViewName, longViewDef, tx9)
		require.NoError(t, err, "Should create view with long definition successfully")

		retrievedLongDef, err := vm.GetViewDef(longViewName, tx9)
		require.NoError(t, err, "Should retrieve long view definition")
		assert.Equal(t, longViewDef, retrievedLongDef, "Long view definition should match")
	}

	tx9.Commit()

	// Test 10: Test case sensitivity
	tx10 := transaction.NewTransaction(fm, lm, bm, lockTable)

	// Create views with similar names but different cases
	err = vm.CreateView("TestView", "SELECT * FROM test", tx10)
	require.NoError(t, err, "Should create TestView")

	err = vm.CreateView("testview", "SELECT * FROM test2", tx10)
	require.NoError(t, err, "Should create testview")

	// Verify they are treated as different views
	testViewDef, err := vm.GetViewDef("TestView", tx10)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test", testViewDef)

	testviewDef, err := vm.GetViewDef("testview", tx10)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test2", testviewDef)

	// Verify non-existent case variations return empty
	nonExistentDef, err := vm.GetViewDef("TESTVIEW", tx10)
	require.NoError(t, err)
	assert.Empty(t, nonExistentDef, "Should return empty for non-existent case variation")

	tx10.Commit()
}
