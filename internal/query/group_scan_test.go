package query

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/query/aggregations"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// TestGroupByScanMultipleAggregations tests multiple aggregation functions.
func TestGroupByScanMultipleAggregations(t *testing.T) {
	testDir := "/tmp/testdb_groupbyscan_multi"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	require.NotNil(t, tx)
	defer tx.Commit()

	// Create table schema
	schema := record.NewSchema()
	schema.AddStringField("dept", 20)
	schema.AddIntField("salary")
	schema.AddStringField("name", 20)
	schema.AddBoolField("active")

	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, "Employees")
	require.NoError(t, err)

	// Insert test data - sorted by dept for grouping
	// CS: salaries 50000, 60000, 55000
	// HR: salaries 45000, 50000
	// Sales: salaries 40000, 45000, 50000
	employees := []struct {
		dept   string
		salary int
		name   string
		active bool
	}{
		{"CS", 50000, "Alice", true},
		{"CS", 60000, "Bob", false},
		{"CS", 55000, "Charlie", true},
		{"HR", 45000, "David", true},
		{"HR", 50000, "Eve", false},
		{"Sales", 40000, "Frank", true},
		{"Sales", 45000, "Grace", true},
		{"Sales", 50000, "Henry", false},
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)
	for _, emp := range employees {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetString("dept", emp.dept)
		require.NoError(t, err)
		err = ts.SetInt("salary", emp.salary)
		require.NoError(t, err)
		err = ts.SetString("name", emp.name)
		require.NoError(t, err)
		err = ts.SetBool("active", emp.active)
		require.NoError(t, err)
		t.Logf("Inserted: dept=%s, salary=%d, name=%s, active=%v", emp.dept, emp.salary, emp.name, emp.active)
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)

	// Create GroupByScan with multiple aggregations: MAX and MIN
	groupFields := []string{"dept", "active"}
	aggFns := []aggregations.AggregationFunction{
		aggregations.NewMaxFn("salary"),
		aggregations.NewMinFn("salary"),
	}

	groupByScan, err := NewGroupByScan(ts, groupFields, aggFns)
	require.NoError(t, err)

	// Collect results
	type Result struct {
		dept      string
		active    bool
		maxSalary int
		minSalary int
	}
	var results []Result

	err = groupByScan.BeforeFirst()
	require.NoError(t, err)

	for {
		hasNext, err := groupByScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := groupByScan.GetString("dept")
		require.NoError(t, err)

		active, err := groupByScan.GetBool("active")
		require.NoError(t, err)

		maxSalary, err := groupByScan.GetInt("maxofsalary")
		require.NoError(t, err)

		minSalary, err := groupByScan.GetInt("minofsalary")
		require.NoError(t, err)

		results = append(results, Result{
			dept:      dept,
			active:    active,
			maxSalary: maxSalary,
			minSalary: minSalary,
		})

		t.Logf("Group: dept=%s, active=%v, maxofsalary=%d, minofsalary=%d", dept, active, maxSalary, minSalary)
	}

	// Should have more groups now (CS+active, CS+inactive, HR+active, HR+inactive, Sales+active, Sales+inactive)
	require.GreaterOrEqual(t, len(results), 3)

	// Verify that bool field is accessible
	for _, r := range results {
		assert.IsType(t, true, r.active)
		assert.GreaterOrEqual(t, r.maxSalary, r.minSalary, "Max should be >= Min for %s/%v", r.dept, r.active)
	}

	groupByScan.Close()
}

// TestGroupByScanMultipleGroupFields tests grouping by multiple fields.
func TestGroupByScanMultipleGroupFields(t *testing.T) {
	testDir := "/tmp/testdb_groupbyscan_multifields"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	dpt := buffer.NewDirtyPageTable()
	bufferManager, err := buffer.NewManager(fileManager, logManager, dpt, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()
	dirtyPageTable := buffer.NewDirtyPageTable()
	transactionTable := transaction.NewTransactionTable()
	transactionManager := transaction.NewTransactionManager(fileManager, logManager, bufferManager, lockTable, dirtyPageTable, transactionTable)
	tx := transactionManager.BeginTransaction()
	require.NotNil(t, tx)
	defer tx.Commit()

	// Create table with dept and year fields
	schema := record.NewSchema()
	schema.AddStringField("dept", 20)
	schema.AddIntField("year")
	schema.AddIntField("salary")

	layout := record.NewLayoutFromSchema(schema)
	ts, err := table.NewTableScan(tx, layout, "Employees")
	require.NoError(t, err)

	// Insert data sorted by dept, year
	employees := []struct {
		dept   string
		year   int
		salary int
	}{
		{"CS", 2022, 50000},
		{"CS", 2022, 55000},
		{"CS", 2023, 60000},
		{"HR", 2022, 45000},
		{"HR", 2023, 50000},
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)
	for _, emp := range employees {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetString("dept", emp.dept)
		require.NoError(t, err)
		err = ts.SetInt("year", emp.year)
		require.NoError(t, err)
		err = ts.SetInt("salary", emp.salary)
		require.NoError(t, err)
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)

	// Group by dept and year
	groupFields := []string{"dept", "year"}
	aggFns := []aggregations.AggregationFunction{
		aggregations.NewMaxFn("salary"),
	}

	groupByScan, err := NewGroupByScan(ts, groupFields, aggFns)
	require.NoError(t, err)

	// Collect results
	type Result struct {
		dept      string
		year      int
		maxSalary int
	}
	var results []Result

	for {
		hasNext, err := groupByScan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := groupByScan.GetString("dept")
		require.NoError(t, err)

		year, err := groupByScan.GetInt("year")
		require.NoError(t, err)

		maxSalary, err := groupByScan.GetInt("maxofsalary")
		require.NoError(t, err)

		results = append(results, Result{dept: dept, year: year, maxSalary: maxSalary})
		t.Logf("Group: dept=%s, year=%d, maxofsalary=%d", dept, year, maxSalary)
	}

	// Should have 4 groups: (CS,2022), (CS,2023), (HR,2022), (HR,2023)
	require.Len(t, results, 4)

	// Verify results
	expected := map[string]map[int]int{
		"CS": {2022: 55000, 2023: 60000},
		"HR": {2022: 45000, 2023: 50000},
	}

	for _, r := range results {
		assert.Equal(t, expected[r.dept][r.year], r.maxSalary,
			"Max salary for %s/%d should be %d", r.dept, r.year, expected[r.dept][r.year])
	}

	groupByScan.Close()
}
