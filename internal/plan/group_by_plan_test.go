package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/query/aggregations"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

func TestGroupByPlan(t *testing.T) {
	_, tx, md, cleanup := setupTestDB(t)
	defer cleanup()

	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddIntField("dept")
	schema.AddIntField("salary")

	tableName := "employees"
	layout := record.NewLayoutFromSchema(schema)
	err := md.CreateTable(tableName, schema, tx)
	require.NoError(t, err)

	ts, err := table.NewTableScan(tx, layout, tableName)
	require.NoError(t, err)
	err = ts.BeforeFirst()
	require.NoError(t, err)

	for i := 1; i <= 12; i++ {
		err = ts.Insert()
		require.NoError(t, err)
		err = ts.SetInt("id", i)
		require.NoError(t, err)
		err = ts.SetInt("dept", i%3)
		require.NoError(t, err)
		err = ts.SetInt("salary", 1000+(i*100))
		require.NoError(t, err)
	}
	ts.Close()

	tablePlan, err := NewTablePlan(tableName, tx, md)
	require.NoError(t, err)

	aggFns := []aggregations.AggregationFunction{
		aggregations.NewMaxFn("salary"),
		aggregations.NewMinFn("salary"),
	}
	groupFields := []string{"dept"}
	groupByPlan := NewGroupByPlan(tx, tablePlan, groupFields, aggFns)

	resultSchema := groupByPlan.Schema()
	require.NotNil(t, resultSchema)
	assert.True(t, resultSchema.HasField("dept"))
	assert.True(t, resultSchema.HasField("maxofsalary"))
	assert.True(t, resultSchema.HasField("minofsalary"))
	assert.False(t, resultSchema.HasField("id"))

	assert.True(t, groupByPlan.BlocksAccessed() >= tablePlan.BlocksAccessed())
	assert.Equal(t, 3, groupByPlan.RecordsOutput())

	distinctDept, err := groupByPlan.DistinctValues("dept")
	require.NoError(t, err)
	tableDistinctDept, err := tablePlan.DistinctValues("dept")
	require.NoError(t, err)
	assert.Equal(t, tableDistinctDept, distinctDept)

	distinctMaxSalary, err := groupByPlan.DistinctValues("maxofsalary")
	require.NoError(t, err)
	assert.Equal(t, 3, distinctMaxSalary)

	scan, err := groupByPlan.Open()
	require.NoError(t, err)
	require.NotNil(t, scan)
	defer scan.Close()

	// Expected results per department:
	// dept 0: salaries 1300, 1600, 1900, 2200 -> max=2200, min=1300
	// dept 1: salaries 1100, 1400, 1700, 2000 -> max=2000, min=1100
	// dept 2: salaries 1200, 1500, 1800, 2100 -> max=2100, min=1200
	expectedResults := map[int]struct {
		maxSalary int
		minSalary int
	}{
		0: {maxSalary: 2200, minSalary: 1300},
		1: {maxSalary: 2000, minSalary: 1100},
		2: {maxSalary: 2100, minSalary: 1200},
	}

	groupCount := 0
	for {
		hasNext, err := scan.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}

		dept, err := scan.GetInt("dept")
		require.NoError(t, err)

		maxSalary, err := scan.GetInt("maxofsalary")
		require.NoError(t, err)

		minSalary, err := scan.GetInt("minofsalary")
		require.NoError(t, err)

		expected, exists := expectedResults[dept]
		require.True(t, exists, "Unexpected department: %d", dept)
		assert.Equal(t, expected.maxSalary, maxSalary, "Max salary for dept %d", dept)
		assert.Equal(t, expected.minSalary, minSalary, "Min salary for dept %d", dept)

		delete(expectedResults, dept)
		groupCount++
	}

	assert.Equal(t, 3, groupCount, "Should have 3 groups")
	assert.Empty(t, expectedResults, "All expected groups should have been found")
}
