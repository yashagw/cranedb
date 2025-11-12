package scan

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

// setupProductScanTest creates two test tables for cartesian product testing
func setupProductScanTest(t *testing.T, testDir string) (*transaction.Transaction, *TableScan, *TableScan) {
	// Setup database components
	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	// Create first table (Students)
	schema1 := record.NewSchema()
	schema1.AddIntField("student_id")
	schema1.AddStringField("name", 20)

	layout1 := record.NewLayoutFromSchema(schema1)
	ts1, err := NewTableScan(tx, layout1, "Students")
	require.NoError(t, err)

	// Insert student data
	students := []struct {
		id   int
		name string
	}{
		{1, "Alice"},
		{2, "Bob"},
		{3, "Charlie"},
	}

	err = ts1.BeforeFirst()
	require.NoError(t, err)
	for _, student := range students {
		err = ts1.Insert()
		require.NoError(t, err)
		err = ts1.SetInt("student_id", student.id)
		require.NoError(t, err)
		err = ts1.SetString("name", student.name)
		require.NoError(t, err)
		t.Logf("Inserted student: id=%d, name=%s", student.id, student.name)
	}

	// Create second table (Courses)
	schema2 := record.NewSchema()
	schema2.AddIntField("course_id")
	schema2.AddStringField("course_name", 20)

	layout2 := record.NewLayoutFromSchema(schema2)
	ts2, err := NewTableScan(tx, layout2, "Courses")
	require.NoError(t, err)

	// Insert course data
	courses := []struct {
		id   int
		name string
	}{
		{101, "Math"},
		{102, "Science"},
	}

	err = ts2.BeforeFirst()
	require.NoError(t, err)
	for _, course := range courses {
		err = ts2.Insert()
		require.NoError(t, err)
		err = ts2.SetInt("course_id", course.id)
		require.NoError(t, err)
		err = ts2.SetString("course_name", course.name)
		require.NoError(t, err)
		t.Logf("Inserted course: id=%d, name=%s", course.id, course.name)
	}

	return tx, ts1, ts2
}

// TestProductScanCartesianProduct tests the basic cartesian product functionality
func TestProductScanCartesianProduct(t *testing.T) {
	testDir := "/tmp/testdb_productscan_cartesian"
	defer os.RemoveAll(testDir)

	tx, ts1, ts2 := setupProductScanTest(t, testDir)
	defer tx.Commit()

	// Create ProductScan
	err := ts1.BeforeFirst()
	require.NoError(t, err)
	err = ts2.BeforeFirst()
	require.NoError(t, err)
	productScan := NewProductScan(ts1, ts2)
	require.NotNil(t, productScan)

	// Collect all combinations
	type Result struct {
		studentID   int
		studentName string
		courseID    int
		courseName  string
	}
	var results []Result

	err = productScan.BeforeFirst()
	require.NoError(t, err)
	for {
		hasNext, err := productScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		studentID, err := productScan.GetInt("student_id")
		require.NoError(t, err)
		studentName, err := productScan.GetString("name")
		require.NoError(t, err)
		courseID, err := productScan.GetInt("course_id")
		require.NoError(t, err)
		courseName, err := productScan.GetString("course_name")
		require.NoError(t, err)

		results = append(results, Result{
			studentID:   studentID,
			studentName: studentName,
			courseID:    courseID,
			courseName:  courseName,
		})
		t.Logf("Product: student=%d(%s), course=%d(%s)", studentID, studentName, courseID, courseName)
	}

	// Verify cartesian product: 3 students x 2 courses = 6 combinations
	require.Len(t, results, 6, "Should have 6 combinations (3 students x 2 courses)")

	// Verify specific combinations exist
	expectedCombos := map[string]bool{
		"Alice-Math":      false,
		"Alice-Science":   false,
		"Bob-Math":        false,
		"Bob-Science":     false,
		"Charlie-Math":    false,
		"Charlie-Science": false,
	}

	for _, r := range results {
		key := r.studentName + "-" + r.courseName
		expectedCombos[key] = true
	}

	for combo, found := range expectedCombos {
		assert.True(t, found, "Combination %s should exist", combo)
	}

	productScan.Close()
}

// TestProductScanFieldAccess tests accessing fields from both scans
func TestProductScanFieldAccess(t *testing.T) {
	testDir := "/tmp/testdb_productscan_fields"
	defer os.RemoveAll(testDir)

	tx, ts1, ts2 := setupProductScanTest(t, testDir)
	defer tx.Commit()

	err := ts1.BeforeFirst()
	require.NoError(t, err)
	err = ts2.BeforeFirst()
	require.NoError(t, err)
	productScan := NewProductScan(ts1, ts2)

	t.Run("HasField", func(t *testing.T) {
		// Fields from scan1
		assert.True(t, productScan.HasField("student_id"))
		assert.True(t, productScan.HasField("name"))

		// Fields from scan2
		assert.True(t, productScan.HasField("course_id"))
		assert.True(t, productScan.HasField("course_name"))

		// Non-existent field
		assert.False(t, productScan.HasField("missing"))

		t.Log("HasField correctly identifies fields from both scans")
	})

	t.Run("GetIntFromBothScans", func(t *testing.T) {
		err := productScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := productScan.Next()
		require.NoError(t, err)
		if hasNext {
			// Get int from scan1
			studentID, err := productScan.GetInt("student_id")
			require.NoError(t, err)
			assert.Greater(t, studentID, 0)
			t.Logf("Got student_id from scan1: %d", studentID)

			// Get int from scan2
			courseID, err := productScan.GetInt("course_id")
			require.NoError(t, err)
			assert.Greater(t, courseID, 0)
			t.Logf("Got course_id from scan2: %d", courseID)
		}
	})

	t.Run("GetStringFromBothScans", func(t *testing.T) {
		err := productScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := productScan.Next()
		require.NoError(t, err)
		if hasNext {
			// Get string from scan1
			studentName, err := productScan.GetString("name")
			require.NoError(t, err)
			assert.NotEmpty(t, studentName)
			t.Logf("Got name from scan1: %s", studentName)

			// Get string from scan2
			courseName, err := productScan.GetString("course_name")
			require.NoError(t, err)
			assert.NotEmpty(t, courseName)
			t.Logf("Got course_name from scan2: %s", courseName)
		}
	})

	t.Run("GetValue", func(t *testing.T) {
		err := productScan.BeforeFirst()
		require.NoError(t, err)
		hasNext, err := productScan.Next()
		require.NoError(t, err)
		if hasNext {
			// GetValue from scan1
			studentIDVal, err := productScan.GetValue("student_id")
			require.NoError(t, err)
			require.NotNil(t, studentIDVal)
			t.Logf("Got student_id via GetValue: %v", studentIDVal)

			// GetValue from scan2
			courseIDVal, err := productScan.GetValue("course_id")
			require.NoError(t, err)
			require.NotNil(t, courseIDVal)
			t.Logf("Got course_id via GetValue: %v", courseIDVal)
		}
	})

	productScan.Close()
}

// TestProductScanNavigation tests BeforeFirst and Next operations
func TestProductScanNavigation(t *testing.T) {
	testDir := "/tmp/testdb_productscan_navigation"
	defer os.RemoveAll(testDir)

	tx, ts1, ts2 := setupProductScanTest(t, testDir)
	defer tx.Commit()

	err := ts1.BeforeFirst()
	require.NoError(t, err)
	err = ts2.BeforeFirst()
	require.NoError(t, err)
	productScan := NewProductScan(ts1, ts2)

	t.Run("IterationOrder", func(t *testing.T) {
		// Product scan should iterate: for each record in scan1, iterate all records in scan2
		err := productScan.BeforeFirst()
		require.NoError(t, err)

		var order []string
		for {
			hasNext, err := productScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			studentName, err := productScan.GetString("name")
			require.NoError(t, err)
			courseName, err := productScan.GetString("course_name")
			require.NoError(t, err)
			combo := studentName + "-" + courseName
			order = append(order, combo)
			t.Logf("Iteration order: %s", combo)
		}

		// Expected order: Alice with all courses, then Bob with all courses, then Charlie with all courses
		require.Len(t, order, 6)
		assert.Equal(t, "Alice-Math", order[0])
		assert.Equal(t, "Alice-Science", order[1])
		assert.Equal(t, "Bob-Math", order[2])
		assert.Equal(t, "Bob-Science", order[3])
		assert.Equal(t, "Charlie-Math", order[4])
		assert.Equal(t, "Charlie-Science", order[5])
	})

	t.Run("ReIteration", func(t *testing.T) {
		// First iteration
		err := productScan.BeforeFirst()
		require.NoError(t, err)
		count1 := 0
		for {
			hasNext, err := productScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count1++
		}
		t.Logf("First iteration: %d records", count1)

		// Second iteration
		err = productScan.BeforeFirst()
		require.NoError(t, err)
		count2 := 0
		for {
			hasNext, err := productScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count2++
		}
		t.Logf("Second iteration: %d records", count2)

		assert.Equal(t, count1, count2, "Should be able to re-iterate")
		assert.Equal(t, 6, count1)
	})

	productScan.Close()
}

// TestProductScanEmptyScans tests behavior with empty scans
func TestProductScanEmptyScans(t *testing.T) {
	testDir := "/tmp/testdb_productscan_empty"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)
	defer tx.Commit()

	t.Run("BothScansEmpty", func(t *testing.T) {
		// Create two empty tables
		schema1 := record.NewSchema()
		schema1.AddIntField("id1")
		layout1 := record.NewLayoutFromSchema(schema1)
		ts1, err := NewTableScan(tx, layout1, "EmptyBoth1")
		require.NoError(t, err)

		schema2 := record.NewSchema()
		schema2.AddIntField("id2")
		layout2 := record.NewLayoutFromSchema(schema2)
		ts2, err := NewTableScan(tx, layout2, "EmptyBoth2")
		require.NoError(t, err)

		err = ts1.BeforeFirst()
		require.NoError(t, err)
		err = ts2.BeforeFirst()
		require.NoError(t, err)
		productScan := NewProductScan(ts1, ts2)

		err = productScan.BeforeFirst()
		require.NoError(t, err)
		count := 0
		for {
			hasNext, err := productScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 0, count, "Product of two empty scans should be empty")
		t.Log("Both empty scans results in empty product")
		productScan.Close()
	})

	t.Run("EmptySecondScan", func(t *testing.T) {
		schema1 := record.NewSchema()
		schema1.AddIntField("id1")
		layout1 := record.NewLayoutFromSchema(schema1)
		ts1, err := NewTableScan(tx, layout1, "NonEmpty2")
		require.NoError(t, err)

		schema2 := record.NewSchema()
		schema2.AddIntField("id2")
		layout2 := record.NewLayoutFromSchema(schema2)
		ts2, err := NewTableScan(tx, layout2, "Empty2")
		require.NoError(t, err)

		// Add one record to scan1
		err = ts1.BeforeFirst()
		require.NoError(t, err)
		err = ts1.Insert()
		require.NoError(t, err)
		err = ts1.SetInt("id1", 1)
		require.NoError(t, err)

		err = ts1.BeforeFirst()
		require.NoError(t, err)
		err = ts2.BeforeFirst()
		require.NoError(t, err)
		productScan := NewProductScan(ts1, ts2)

		err = productScan.BeforeFirst()
		require.NoError(t, err)
		count := 0
		for {
			hasNext, err := productScan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}
			count++
		}

		assert.Equal(t, 0, count, "Product with empty second scan should be empty")
		t.Log("Empty second scan results in empty product")
		productScan.Close()
	})
}

// TestProductScanWithSelectScan tests combining ProductScan with SelectScan
func TestProductScanWithSelectScan(t *testing.T) {
	testDir := "/tmp/testdb_productscan_select"
	defer os.RemoveAll(testDir)

	tx, ts1, ts2 := setupProductScanTest(t, testDir)
	defer tx.Commit()

	// Create product scan
	err := ts1.BeforeFirst()
	require.NoError(t, err)
	err = ts2.BeforeFirst()
	require.NoError(t, err)
	productScan := NewProductScan(ts1, ts2)

	// Apply a filter: student_id = 1 (Alice only)
	predicate := newTestPredicate("student_id", 1)
	selectScan := NewSelectScan(productScan, predicate)

	err = selectScan.BeforeFirst()
	require.NoError(t, err)
	count := 0
	for {
		hasNext, err := selectScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		studentID, err := selectScan.GetInt("student_id")
		require.NoError(t, err)
		studentName, err := selectScan.GetString("name")
		require.NoError(t, err)
		courseName, err := selectScan.GetString("course_name")
		require.NoError(t, err)

		assert.Equal(t, 1, studentID)
		assert.Equal(t, "Alice", studentName)
		t.Logf("Filtered result: %s enrolled in %s", studentName, courseName)
		count++
	}

	// Alice with 2 courses = 2 results
	assert.Equal(t, 2, count, "Should have 2 records for Alice")
	t.Log("ProductScan with SelectScan works correctly")

	selectScan.Close()
}
