package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
)

func TestSortPlan(t *testing.T) {
	t.Run("BasicSort", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		// Create schema and table
		schema := record.NewSchema()
		schema.AddIntField("id")
		schema.AddStringField("name", 20)
		tableName := "test"
		err := md.CreateTable(tableName, schema, tx)
		require.NoError(t, err)

		// Insert test data in unsorted order
		layout := record.NewLayoutFromSchema(schema)
		ts, err := table.NewTableScan(tx, layout, tableName)
		require.NoError(t, err)

		testData := []struct {
			id   int
			name string
		}{
			{3, "Charlie"},
			{1, "Alice"},
			{4, "David"},
			{2, "Bob"},
		}

		for _, data := range testData {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", data.id)
			require.NoError(t, err)
			err = ts.SetString("name", data.name)
			require.NoError(t, err)
		}
		ts.Close()

		// Create SortPlan
		tablePlan, err := NewTablePlan(tableName, tx, md)
		require.NoError(t, err)
		sortPlan := NewSortPlan(tablePlan, []string{"id"}, tx)

		// Test Schema
		sortedSchema := sortPlan.Schema()
		require.NotNil(t, sortedSchema)
		assert.True(t, sortedSchema.HasField("id"))
		assert.True(t, sortedSchema.HasField("name"))

		// Test RecordsOutput
		assert.Equal(t, 4, sortPlan.RecordsOutput())

		// Test Open and verify sorted order
		scan, err := sortPlan.Open()
		require.NoError(t, err)
		require.NotNil(t, scan)

		expectedIds := []int{1, 2, 3, 4}
		expectedNames := []string{"Alice", "Bob", "Charlie", "David"}
		idx := 0

		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}

			id, err := scan.GetInt("id")
			require.NoError(t, err)
			name, err := scan.GetString("name")
			require.NoError(t, err)

			assert.Equal(t, expectedIds[idx], id, "Records should be sorted by id")
			assert.Equal(t, expectedNames[idx], name)
			idx++
		}
		assert.Equal(t, 4, idx)

		scan.Close()
	})

	t.Run("SortByMultipleFields", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		// Create schema and table
		schema := record.NewSchema()
		schema.AddIntField("age")
		schema.AddStringField("name", 20)
		tableName := "test"
		err := md.CreateTable(tableName, schema, tx)
		require.NoError(t, err)

		// Insert test data
		layout := record.NewLayoutFromSchema(schema)
		ts, err := table.NewTableScan(tx, layout, tableName)
		require.NoError(t, err)

		testData := []struct {
			age  int
			name string
		}{
			{25, "Charlie"},
			{25, "Alice"},
			{30, "Bob"},
			{25, "Bob"},
			{30, "Alice"},
		}

		for _, data := range testData {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("age", data.age)
			require.NoError(t, err)
			err = ts.SetString("name", data.name)
			require.NoError(t, err)
		}
		ts.Close()

		// Create SortPlan sorting by age, then name
		tablePlan, err := NewTablePlan(tableName, tx, md)
		require.NoError(t, err)
		sortPlan := NewSortPlan(tablePlan, []string{"age", "name"}, tx)

		// Test Open and verify sorted order
		scan, err := sortPlan.Open()
		require.NoError(t, err)
		require.NotNil(t, scan)

		expectedRecords := []struct {
			age  int
			name string
		}{
			{25, "Alice"},
			{25, "Bob"},
			{25, "Charlie"},
			{30, "Alice"},
			{30, "Bob"},
		}

		idx := 0
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}

			age, err := scan.GetInt("age")
			require.NoError(t, err)
			name, err := scan.GetString("name")
			require.NoError(t, err)

			assert.Equal(t, expectedRecords[idx].age, age)
			assert.Equal(t, expectedRecords[idx].name, name)
			idx++
		}
		assert.Equal(t, 5, idx)

		scan.Close()
	})

	t.Run("EmptyTable", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		// Create schema and empty table
		schema := record.NewSchema()
		schema.AddIntField("id")
		tableName := "test"
		err := md.CreateTable(tableName, schema, tx)
		require.NoError(t, err)

		// Create SortPlan
		tablePlan, err := NewTablePlan(tableName, tx, md)
		require.NoError(t, err)
		sortPlan := NewSortPlan(tablePlan, []string{"id"}, tx)

		// Test Open with empty table
		scan, err := sortPlan.Open()
		require.NoError(t, err)
		require.NotNil(t, scan)

		// Should have no records
		hasNext, err := scan.Next()
		require.NoError(t, err)
		assert.False(t, hasNext)

		scan.Close()
	})

	t.Run("WithSelectPlan", func(t *testing.T) {
		_, tx, md, cleanup := setupTestDB(t)
		defer cleanup()

		// Create schema and table
		schema := record.NewSchema()
		schema.AddIntField("id")
		schema.AddIntField("age")
		tableName := "test"
		err := md.CreateTable(tableName, schema, tx)
		require.NoError(t, err)

		// Insert test data
		layout := record.NewLayoutFromSchema(schema)
		ts, err := table.NewTableScan(tx, layout, tableName)
		require.NoError(t, err)
		for i := 1; i <= 5; i++ {
			err = ts.Insert()
			require.NoError(t, err)
			err = ts.SetInt("id", i)
			require.NoError(t, err)
			err = ts.SetInt("age", 20+i)
			require.NoError(t, err)
		}
		ts.Close()

		// Create TablePlan -> SelectPlan -> SortPlan
		tablePlan, err := NewTablePlan(tableName, tx, md)
		require.NoError(t, err)

		// Create a predicate: age = 22
		fieldExpr := query.NewFieldNameExpression("age")
		constExpr := query.NewConstantExpression(*query.NewIntConstant(22))
		term := query.NewTerm(*fieldExpr, *constExpr, query.OpEQ)
		pred := query.NewPredicate(*term)
		selectPlan := NewSelectPlan(tablePlan, pred)
		sortPlan := NewSortPlan(selectPlan, []string{"id"}, tx)

		// Test Open
		scan, err := sortPlan.Open()
		require.NoError(t, err)
		require.NotNil(t, scan)

		// Should have filtered and sorted records
		count := 0
		lastId := 0
		for {
			hasNext, err := scan.Next()
			require.NoError(t, err)
			if !hasNext {
				break
			}

			id, err := scan.GetInt("id")
			require.NoError(t, err)
			age, err := scan.GetInt("age")
			require.NoError(t, err)

			assert.Equal(t, 22, age, "Should only have records with age = 22")
			assert.True(t, id >= lastId, "Records should be sorted by id")
			lastId = id
			count++
		}

		scan.Close()
	})
}
