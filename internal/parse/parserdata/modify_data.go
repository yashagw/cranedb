package parserdata

import "github.com/yashagw/cranedb/internal/query"

type ModifyData struct {
	table     string
	fieldName string
	newValue  *query.Expression
	predicate *query.Predicate
}

func NewModifyData(table string, fieldName string, newValue *query.Expression, predicate *query.Predicate) *ModifyData {
	return &ModifyData{
		table:     table,
		fieldName: fieldName,
		newValue:  newValue,
		predicate: predicate,
	}
}

func (u *ModifyData) Table() string {
	return u.table
}

func (u *ModifyData) FieldName() string {
	return u.fieldName
}

func (u *ModifyData) NewValue() *query.Expression {
	return u.newValue
}

func (u *ModifyData) Predicate() *query.Predicate {
	return u.predicate
}
