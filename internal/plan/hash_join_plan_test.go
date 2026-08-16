package plan

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHashJoinPlanSchema verifies the combined schema contains fields from both sides.
func TestHashJoinPlanSchema(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	hjp := NewHashJoinPlan(studentsPlan, departmentsPlan, "dept_id", "dept_id")

	schema := hjp.Schema()
	require.NotNil(t, schema)
	assert.True(t, schema.HasField("id"))
	assert.True(t, schema.HasField("name"))
	assert.True(t, schema.HasField("dept_id"))
	assert.True(t, schema.HasField("dept_name"))
}

// TestHashJoinPlanBlocksAccessed verifies each side is counted exactly once.
func TestHashJoinPlanBlocksAccessed(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	hjp := NewHashJoinPlan(studentsPlan, departmentsPlan, "dept_id", "dept_id")

	expected := studentsPlan.BlocksAccessed() + departmentsPlan.BlocksAccessed()
	assert.Equal(t, expected, hjp.BlocksAccessed())
}

// TestHashJoinPlanRecordsOutput spot-checks the equi-join output estimate.
func TestHashJoinPlanRecordsOutput(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	hjp := NewHashJoinPlan(studentsPlan, departmentsPlan, "dept_id", "dept_id")

	// max distinct across the two join fields
	dvStudents, err := studentsPlan.DistinctValues("dept_id")
	require.NoError(t, err)
	dvDept, err := departmentsPlan.DistinctValues("dept_id")
	require.NoError(t, err)
	maxDistinct := max(dvStudents, dvDept, 1)

	expected := (studentsPlan.RecordsOutput() * departmentsPlan.RecordsOutput()) / maxDistinct
	assert.Equal(t, expected, hjp.RecordsOutput())
}

// TestHashJoinPlanDistinctValues verifies delegation to the child owning the field.
func TestHashJoinPlanDistinctValues(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	hjp := NewHashJoinPlan(studentsPlan, departmentsPlan, "dept_id", "dept_id")

	nameStudents, err := studentsPlan.DistinctValues("name")
	require.NoError(t, err)
	nameHJ, err := hjp.DistinctValues("name")
	require.NoError(t, err)
	assert.Equal(t, nameStudents, nameHJ)

	deptNameDept, err := departmentsPlan.DistinctValues("dept_name")
	require.NoError(t, err)
	deptNameHJ, err := hjp.DistinctValues("dept_name")
	require.NoError(t, err)
	assert.Equal(t, deptNameDept, deptNameHJ)
}

// TestHashJoinPlanBuildSideSelection verifies the smaller input becomes the build side.
// Students has 3 rows, Departments has 2, so Departments must be the build (first) child.
func TestHashJoinPlanBuildSideSelection(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	// Pass the larger plan (Students) first; the plan should still build on Departments.
	hjp := NewHashJoinPlan(studentsPlan, departmentsPlan, "dept_id", "dept_id")

	explain := hjp.Explain("", true)
	lines := strings.Split(explain, "\n")
	require.GreaterOrEqual(t, len(lines), 3)

	// First child (build side, "├─") should be the Departments table.
	assert.Contains(t, lines[1], "Departments", "smaller table should be the build side (first child)")
}

// TestHashJoinPlanOpen verifies full iteration correctness on real tables.
func TestHashJoinPlanOpen(t *testing.T) {
	_, tx, md, cleanup := setupIndexJoinPlanTest(t)
	defer cleanup()

	studentsPlan, err := NewTablePlan("Students", tx, md)
	require.NoError(t, err)
	departmentsPlan, err := NewTablePlan("Departments", tx, md)
	require.NoError(t, err)

	hjp := NewHashJoinPlan(studentsPlan, departmentsPlan, "dept_id", "dept_id")

	s, err := hjp.Open()
	require.NoError(t, err)
	defer s.Close()

	type joinRow struct {
		name     string
		deptName string
	}
	var got []joinRow
	for {
		hasNext, err := s.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		name, err := s.GetString("name")
		require.NoError(t, err)
		deptName, err := s.GetString("dept_name")
		require.NoError(t, err)
		got = append(got, joinRow{name, deptName})
	}

	// Alice->CS, Bob->Math, Charlie->CS (from setupIndexJoinPlanTest data).
	want := []joinRow{
		{"Alice", "CS"},
		{"Bob", "Math"},
		{"Charlie", "CS"},
	}
	assert.ElementsMatch(t, want, got)
}
