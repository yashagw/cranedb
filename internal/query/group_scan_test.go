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
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)
	defer tx.Commit()

	// Create table schema
	schema := record.NewSchema()
	schema.AddStringField("dept", 20)
	schema.AddIntField("salary")
	schema.AddStringField("name", 20)

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
	}{
		{"CS", 50000, "Alice"},
		{"CS", 60000, "Bob"},
		{"CS", 55000, "Charlie"},
		{"HR", 45000, "David"},
		{"HR", 50000, "Eve"},
		{"Sales", 40000, "Frank"},
		{"Sales", 45000, "Grace"},
		{"Sales", 50000, "Henry"},
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
		t.Logf("Inserted: dept=%s, salary=%d, name=%s", emp.dept, emp.salary, emp.name)
	}

	err = ts.BeforeFirst()
	require.NoError(t, err)

	// Create GroupByScan with multiple aggregations: MAX and MIN
	groupFields := []string{"dept"}
	aggFns := []aggregations.AggregationFunction{
		aggregations.NewMaxFn("salary"),
		aggregations.NewMinFn("salary"),
	}

	groupByScan, err := NewGroupByScan(ts, groupFields, aggFns)
	require.NoError(t, err)

	// Collect results
	type Result struct {
		dept      string
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

		maxSalary, err := groupByScan.GetInt("maxofsalary")
		require.NoError(t, err)

		minSalary, err := groupByScan.GetInt("minofsalary")
		require.NoError(t, err)

		results = append(results, Result{
			dept:      dept,
			maxSalary: maxSalary,
			minSalary: minSalary,
		})

		t.Logf("Group: dept=%s, maxofsalary=%d, minofsalary=%d", dept, maxSalary, minSalary)
	}

	// Should have 3 groups: CS, HR, Sales
	require.Len(t, results, 3)

	// Verify results
	expected := map[string]struct {
		max int
		min int
	}{
		"CS":    {max: 60000, min: 50000}, // max of 50000,60000,55000; min of 50000,60000,55000
		"HR":    {max: 50000, min: 45000}, // max of 45000,50000; min of 45000,50000
		"Sales": {max: 50000, min: 40000}, // max of 40000,45000,50000; min of 40000,45000,50000
	}

	for _, r := range results {
		exp := expected[r.dept]
		assert.Equal(t, exp.max, r.maxSalary, "Max salary for %s should be %d", r.dept, exp.max)
		assert.Equal(t, exp.min, r.minSalary, "Min salary for %s should be %d", r.dept, exp.min)
		assert.GreaterOrEqual(t, r.maxSalary, r.minSalary, "Max should be >= Min for %s", r.dept)
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
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
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
