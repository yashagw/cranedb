package parserdata

import "github.com/yashagw/cranedb/internal/record"

type CreateTableData struct {
	tableName string
	schema    *record.Schema
}

func NewCreateTableData(tableName string, schema *record.Schema) *CreateTableData {
	return &CreateTableData{
		tableName: tableName,
		schema:    schema,
	}
}

func (c *CreateTableData) TableName() string {
	return c.tableName
}

func (c *CreateTableData) Schema() *record.Schema {
	return c.schema
}
