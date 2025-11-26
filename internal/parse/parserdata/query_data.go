package parserdata

import (
	"github.com/yashagw/cranedb/internal/query"
)

type QueryData struct {
	fields     []string
	tables     []string
	predicate  *query.Predicate
	sortFields []string
}

func NewQueryData(fields []string, tables []string, predicate *query.Predicate) *QueryData {
	return &QueryData{
		fields:     fields,
		tables:     tables,
		predicate:  predicate,
		sortFields: nil,
	}
}

func NewQueryDataWithSort(fields []string, tables []string, predicate *query.Predicate, sortFields []string) *QueryData {
	return &QueryData{
		fields:     fields,
		tables:     tables,
		predicate:  predicate,
		sortFields: sortFields,
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

func (q *QueryData) SortFields() []string {
	return q.sortFields
}

// String returns a SQL string representation of the query.
func (q *QueryData) String() string {
	result := "SELECT "

	// Add fields
	for i, field := range q.fields {
		if i > 0 {
			result += ", "
		}
		result += field
	}

	// Add tables
	result += " FROM "
	for i, table := range q.tables {
		if i > 0 {
			result += ", "
		}
		result += table
	}

	// Add predicate if present
	if q.predicate != nil && q.predicate.String() != "" {
		result += " WHERE " + q.predicate.String()
	}

	// Add ORDER BY if present
	if len(q.sortFields) > 0 {
		result += " ORDER BY "
		for i, field := range q.sortFields {
			if i > 0 {
				result += ", "
			}
			result += field
		}
	}

	return result
}
