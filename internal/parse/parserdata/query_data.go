package parserdata

import (
	"github.com/yashagw/cranedb/internal/query"
)

type QueryData struct {
	fields    []string
	tables    []string
	predicate *query.Predicate
}

func NewQueryData(fields []string, tables []string, predicate *query.Predicate) *QueryData {
	return &QueryData{
		fields:    fields,
		tables:    tables,
		predicate: predicate,
	}
}

func (q *QueryData) Fields() []string {
	return q.fields
}

func (q *QueryData) Tables() []string {
	return q.tables
}

func (q *QueryData) Predicate() *query.Predicate {
	return q.predicate
}
