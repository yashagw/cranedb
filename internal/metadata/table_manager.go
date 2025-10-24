package metadata

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

const (
	TableCatalogName = "table_catelog"
	FieldCatalogName = "field_catelog"
	MaxStringSize    = 16
)

type TableManager struct {
	tableCatelog *record.Layout
	fieldCatelog *record.Layout
}

func NewTableManager(isNew bool, tx *transaction.Transaction) *TableManager {
	tableSchema := record.NewSchema()
	tableSchema.AddStringField("table_name", MaxStringSize)
	tableSchema.AddIntField("slot_size")
	tableLayout := record.NewLayoutFromSchema(tableSchema)

	fieldSchema := record.NewSchema()
	fieldSchema.AddStringField("table_name", MaxStringSize)
	fieldSchema.AddStringField("field_name", MaxStringSize)
	fieldSchema.AddStringField("type", MaxStringSize)
	fieldSchema.AddIntField("length")
	fieldSchema.AddIntField("offset")
	fieldLayout := record.NewLayoutFromSchema(fieldSchema)

	tm := &TableManager{
		tableCatelog: tableLayout,
		fieldCatelog: fieldLayout,
	}

	if isNew {
		tm.CreateTable(TableCatalogName, tableSchema, tx)
		tm.CreateTable(FieldCatalogName, fieldSchema, tx)
	}

	return tm
}

// CreateTable creates a new table in the database by inserting a record into the tableCatelog and fieldCatelog
func (t *TableManager) CreateTable(tableName string, schema *record.Schema, tx *transaction.Transaction) error {
	layout := record.NewLayoutFromSchema(schema)

	// Insert a record into tableCatelog
	tcat := record.NewTableScan(tx, t.tableCatelog, TableCatalogName)
	defer tcat.Close()
	tcat.Insert()
	tcat.SetString("table_name", tableName)
	tcat.SetInt("slot_size", layout.GetSlotSize())

	// Insert a record into fieldCatelog for each field
	fcat := record.NewTableScan(tx, t.fieldCatelog, FieldCatalogName)
	defer fcat.Close()
	for _, fieldName := range schema.Fields() {
		fcat.Insert()
		fcat.SetString("table_name", tableName)
		fcat.SetString("field_name", fieldName)
		fcat.SetString("type", schema.Type(fieldName))
		fcat.SetInt("length", schema.Length(fieldName))
		fcat.SetInt("offset", layout.GetOffset(fieldName))
	}
	return nil
}

// GetLayout retrieves the layout for a given table name by scanning the catalogs
func (t *TableManager) GetLayout(tableName string, tx *transaction.Transaction) (*record.Layout, error) {
	// First, find the slot size from table catalog
	slotSize := -1
	tcat := record.NewTableScan(tx, t.tableCatelog, TableCatalogName)
	defer tcat.Close()

	for tcat.Next() {
		if tcat.GetString("table_name") == tableName {
			slotSize = tcat.GetInt("slot_size")
			break
		}
	}

	if slotSize == -1 {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Build schema and offsets from field catalog
	schema := record.NewSchema()
	offsets := make(map[string]int)

	fcat := record.NewTableScan(tx, t.fieldCatelog, FieldCatalogName)
	defer fcat.Close()

	for fcat.Next() {
		if fcat.GetString("table_name") == tableName {
			fieldName := fcat.GetString("field_name")
			fieldType := fcat.GetString("type")
			fieldLength := fcat.GetInt("length")
			offset := fcat.GetInt("offset")

			offsets[fieldName] = offset
			if fieldType == "int" {
				schema.AddIntField(fieldName)
			} else if fieldType == "string" {
				schema.AddStringField(fieldName, fieldLength)
			}
		}
	}

	return record.NewLayout(schema, offsets, slotSize), nil
}
