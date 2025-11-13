package metadata

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
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
	tcat, err := table.NewTableScan(tx, t.tableCatelog, TableCatalogName)
	if err != nil {
		return err
	}
	defer tcat.Close()
	err = tcat.Insert()
	if err != nil {
		return err
	}
	err = tcat.SetString("table_name", tableName)
	if err != nil {
		return err
	}
	err = tcat.SetInt("slot_size", layout.GetSlotSize())
	if err != nil {
		return err
	}

	// Insert a record into fieldCatelog for each field
	fcat, err := table.NewTableScan(tx, t.fieldCatelog, FieldCatalogName)
	if err != nil {
		return err
	}
	defer fcat.Close()
	for _, fieldName := range schema.Fields() {
		err = fcat.Insert()
		if err != nil {
			return err
		}
		err = fcat.SetString("table_name", tableName)
		if err != nil {
			return err
		}
		err = fcat.SetString("field_name", fieldName)
		if err != nil {
			return err
		}
		err = fcat.SetString("type", schema.Type(fieldName))
		if err != nil {
			return err
		}
		err = fcat.SetInt("length", schema.Length(fieldName))
		if err != nil {
			return err
		}
		err = fcat.SetInt("offset", layout.GetOffset(fieldName))
		if err != nil {
			return err
		}
	}
	return nil
}

// GetLayout retrieves the layout for a given table name by scanning the catalogs
func (t *TableManager) GetLayout(tableName string, tx *transaction.Transaction) (*record.Layout, error) {
	// First, find the slot size from table catalog
	slotSize := -1
	tcat, err := table.NewTableScan(tx, t.tableCatelog, TableCatalogName)
	if err != nil {
		return nil, err
	}
	defer tcat.Close()

	for {
		hasNext, err := tcat.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}
		tableNameVal, err := tcat.GetString("table_name")
		if err != nil {
			return nil, err
		}
		if tableNameVal == tableName {
			slotSizeVal, err := tcat.GetInt("slot_size")
			if err != nil {
				return nil, err
			}
			slotSize = slotSizeVal
			break
		}
	}

	if slotSize == -1 {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	// Build schema and offsets from field catalog
	schema := record.NewSchema()
	offsets := make(map[string]int)

	fcat, err := table.NewTableScan(tx, t.fieldCatelog, FieldCatalogName)
	if err != nil {
		return nil, err
	}
	defer fcat.Close()

	for {
		hasNext, err := fcat.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}
		tableNameVal, err := fcat.GetString("table_name")
		if err != nil {
			return nil, err
		}
		if tableNameVal == tableName {
			fieldName, err := fcat.GetString("field_name")
			if err != nil {
				return nil, err
			}
			fieldType, err := fcat.GetString("type")
			if err != nil {
				return nil, err
			}
			fieldLength, err := fcat.GetInt("length")
			if err != nil {
				return nil, err
			}
			offset, err := fcat.GetInt("offset")
			if err != nil {
				return nil, err
			}

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
