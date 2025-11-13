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
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestTableManager_BasicOperations(t *testing.T) {
	dbDir := "testdata"
	blockSize := 400

	fm, err := file.NewManager(dbDir, blockSize)
	assert.NoError(t, err)
	defer fm.Close()
	defer os.RemoveAll(dbDir)

	lm, err := log.NewManager(fm, "testlog")
	assert.NoError(t, err)
	defer lm.Close()

	bm, err := buffer.NewManager(fm, lm, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	// Test 1: Create new TableManager (isNew = true)
	tx1 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm := NewTableManager(true, tx1)
	require.NotNil(t, tm)
	assert.NotNil(t, tm.tableCatelog)
	assert.NotNil(t, tm.fieldCatelog)
	tx1.Commit()

	// Test 2: Create TableManager for existing database (isNew = false)
	tx2 := transaction.NewTransaction(fm, lm, bm, lockTable)
	tm2 := NewTableManager(false, tx2)
	require.NotNil(t, tm2)
	assert.NotNil(t, tm2.tableCatelog)
	assert.NotNil(t, tm2.fieldCatelog)
	tx2.Commit()

	// Test 3: Create a new table
	tx3 := transaction.NewTransaction(fm, lm, bm, lockTable)
	schema := record.NewSchema()
	schema.AddIntField("id")
	schema.AddStringField("name", 50)
	schema.AddStringField("email", 100)
	err = tm.CreateTable("users", schema, tx3)
	require.NoError(t, err, "Should create table successfully")
	tx3.Commit()

	// Test 4: Retrieve table layout and verify it matches original schema
	tx4 := transaction.NewTransaction(fm, lm, bm, lockTable)
	layout, err := tm.GetLayout("users", tx4)
	require.NoError(t, err, "Should retrieve layout successfully")
	require.NotNil(t, layout)

	retrievedSchema := layout.GetSchema()

	// Verify schema fields match exactly
	assert.Equal(t, schema.Fields(), retrievedSchema.Fields(), "Retrieved schema should have same fields as original")

	// Verify each field's type and length matches the original schema
	for _, fieldName := range schema.Fields() {
		assert.Equal(t, schema.Type(fieldName), retrievedSchema.Type(fieldName),
			"Field %s type should match", fieldName)
		assert.Equal(t, schema.Length(fieldName), retrievedSchema.Length(fieldName),
			"Field %s length should match", fieldName)
	}

	// Verify offsets are set correctly (should be in same order as original schema)
	expectedLayout := record.NewLayoutFromSchema(schema)
	for _, fieldName := range schema.Fields() {
		assert.Equal(t, expectedLayout.GetOffset(fieldName), layout.GetOffset(fieldName),
			"Field %s offset should match", fieldName)
	}

	// Verify slot size matches what would be calculated from original schema
	assert.Equal(t, expectedLayout.GetSlotSize(), layout.GetSlotSize(),
		"Retrieved slot size should match calculated slot size from original schema")

	tx4.Commit()

	// Test 5: Try to get layout for non-existent table
	tx5 := transaction.NewTransaction(fm, lm, bm, lockTable)
	_, err = tm.GetLayout("nonexistent", tx5)
	require.Error(t, err, "Should return error for non-existent table")
	assert.Contains(t, err.Error(), "not found")
	tx5.Commit()

	// Test 6: Create another table with different schema
	tx6 := transaction.NewTransaction(fm, lm, bm, lockTable)

	productSchema := record.NewSchema()
	productSchema.AddStringField("product_id", 20)
	productSchema.AddStringField("description", 200)
	productSchema.AddIntField("price")

	err = tm.CreateTable("products", productSchema, tx6)
	require.NoError(t, err, "Should create second table successfully")
	tx6.Commit()

	// Test 7: Verify both tables exist and have correct layouts
	tx7 := transaction.NewTransaction(fm, lm, bm, lockTable)

	// Get users layout
	usersLayout, err := tm.GetLayout("users", tx7)
	require.NoError(t, err)
	usersSchema := usersLayout.GetSchema()
	assert.Equal(t, 3, len(usersSchema.Fields()))

	// Get products layout
	productsLayout, err := tm.GetLayout("products", tx7)
	require.NoError(t, err)
	productsSchema := productsLayout.GetSchema()
	assert.Equal(t, 3, len(productsSchema.Fields()))

	tx7.Commit()

	// Test 8: Verify catalog data by directly scanning the catalog tables
	tx8 := transaction.NewTransaction(fm, lm, bm, lockTable)

	// Verify table catalog contains correct data for both tables
	tcat, err := table.NewTableScan(tx8, tm.tableCatelog, TableCatalogName)
	require.NoError(t, err)
	defer tcat.Close()

	usersTableFound := false
	productsTableFound := false
	expectedUsersSlotSize := record.NewLayoutFromSchema(schema).GetSlotSize()
	expectedProductsSlotSize := record.NewLayoutFromSchema(productSchema).GetSlotSize()

	for {
		hasNext, err := tcat.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		tableName, err := tcat.GetString("table_name")
		require.NoError(t, err)
		slotSize, err := tcat.GetInt("slot_size")
		require.NoError(t, err)

		if tableName == "users" {
			usersTableFound = true
			assert.Equal(t, expectedUsersSlotSize, slotSize, "Users table slot size should match")
		} else if tableName == "products" {
			productsTableFound = true
			assert.Equal(t, expectedProductsSlotSize, slotSize, "Products table slot size should match")
		}
	}
	assert.True(t, usersTableFound, "Users table should be found in table catalog")
	assert.True(t, productsTableFound, "Products table should be found in table catalog")

	// Verify field catalog contains correct data for both tables
	fcat, err := table.NewTableScan(tx8, tm.fieldCatelog, FieldCatalogName)
	require.NoError(t, err)
	defer fcat.Close()

	usersFieldRecords := make(map[string]map[string]interface{})
	productsFieldRecords := make(map[string]map[string]interface{})

	for {
		hasNext, err := fcat.Next()
		require.NoError(t, err)
		if !hasNext {
			break
		}
		tableName, err := fcat.GetString("table_name")
		require.NoError(t, err)
		fieldName, err := fcat.GetString("field_name")
		require.NoError(t, err)
		fieldType, err := fcat.GetString("type")
		require.NoError(t, err)
		fieldLength, err := fcat.GetInt("length")
		require.NoError(t, err)
		fieldOffset, err := fcat.GetInt("offset")
		require.NoError(t, err)

		fieldData := map[string]interface{}{
			"type":   fieldType,
			"length": fieldLength,
			"offset": fieldOffset,
		}

		if tableName == "users" {
			usersFieldRecords[fieldName] = fieldData
		} else if tableName == "products" {
			productsFieldRecords[fieldName] = fieldData
		}
	}

	// Verify users table fields
	assert.Len(t, usersFieldRecords, 3, "Users table should have 3 fields")
	assert.Contains(t, usersFieldRecords, "id")
	assert.Contains(t, usersFieldRecords, "name")
	assert.Contains(t, usersFieldRecords, "email")

	// Verify users field data matches original schema
	expectedUsersLayout := record.NewLayoutFromSchema(schema)
	assert.Equal(t, "int", usersFieldRecords["id"]["type"])
	assert.Equal(t, 4, usersFieldRecords["id"]["length"])
	assert.Equal(t, expectedUsersLayout.GetOffset("id"), usersFieldRecords["id"]["offset"])

	assert.Equal(t, "string", usersFieldRecords["name"]["type"])
	assert.Equal(t, 50, usersFieldRecords["name"]["length"])
	assert.Equal(t, expectedUsersLayout.GetOffset("name"), usersFieldRecords["name"]["offset"])

	assert.Equal(t, "string", usersFieldRecords["email"]["type"])
	assert.Equal(t, 100, usersFieldRecords["email"]["length"])
	assert.Equal(t, expectedUsersLayout.GetOffset("email"), usersFieldRecords["email"]["offset"])

	// Verify products table fields
	assert.Len(t, productsFieldRecords, 3, "Products table should have 3 fields")
	assert.Contains(t, productsFieldRecords, "product_id")
	assert.Contains(t, productsFieldRecords, "description")
	assert.Contains(t, productsFieldRecords, "price")

	// Verify products field data matches original schema
	expectedProductsLayout := record.NewLayoutFromSchema(productSchema)
	assert.Equal(t, "string", productsFieldRecords["product_id"]["type"])
	assert.Equal(t, 20, productsFieldRecords["product_id"]["length"])
	assert.Equal(t, expectedProductsLayout.GetOffset("product_id"), productsFieldRecords["product_id"]["offset"])

	assert.Equal(t, "string", productsFieldRecords["description"]["type"])
	assert.Equal(t, 200, productsFieldRecords["description"]["length"])
	assert.Equal(t, expectedProductsLayout.GetOffset("description"), productsFieldRecords["description"]["offset"])

	assert.Equal(t, "int", productsFieldRecords["price"]["type"])
	assert.Equal(t, 4, productsFieldRecords["price"]["length"])
	assert.Equal(t, expectedProductsLayout.GetOffset("price"), productsFieldRecords["price"]["offset"])

	tx8.Commit()
}
