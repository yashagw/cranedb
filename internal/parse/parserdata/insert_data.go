package parserdata

type InsertData struct {
	table  string
	fields []string
	values []any
}

func NewInsertData(table string, fields []string, values []any) *InsertData {
	return &InsertData{
		table:  table,
		fields: fields,
		values: values,
	}
}

func (i *InsertData) Table() string {
	return i.table
}

func (i *InsertData) Fields() []string {
	return i.fields
}

func (i *InsertData) Values() []any {
	return i.values
}
