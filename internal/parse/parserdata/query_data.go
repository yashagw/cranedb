package parserdata

import (
	"github.com/yashagw/cranedb/internal/query"
)

type AggregationType string

const (
	AggMax AggregationType = "max"
	AggMin AggregationType = "min"
)

type AggregationFn struct {
	Type      AggregationType
	FieldName string
}

type QueryData struct {
	fields         []string
	tables         []string
	predicate      *query.Predicate
	sortFields     []string
	groupFields    []string
	aggregationFns []AggregationFn
}

func NewQueryData(fields []string, tables []string, predicate *query.Predicate) *QueryData {
	return &QueryData{
		fields:         fields,
		tables:         tables,
		predicate:      predicate,
		sortFields:     nil,
		groupFields:    nil,
		aggregationFns: nil,
	}
}

func NewQueryDataWithSort(fields []string, tables []string, predicate *query.Predicate, sortFields []string) *QueryData {
	return &QueryData{
		fields:         fields,
		tables:         tables,
		predicate:      predicate,
		sortFields:     sortFields,
		groupFields:    nil,
		aggregationFns: nil,
	}
}

func NewQueryDataWithGroupBy(fields []string, tables []string, predicate *query.Predicate, sortFields []string, groupFields []string, aggregationFns []AggregationFn) *QueryData {
	return &QueryData{
		fields:         fields,
		tables:         tables,
		predicate:      predicate,
		sortFields:     sortFields,
		groupFields:    groupFields,
		aggregationFns: aggregationFns,
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

func (q *QueryData) GroupFields() []string {
	return q.groupFields
}

func (q *QueryData) AggregationFns() []AggregationFn {
	return q.aggregationFns
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

	// Add GROUP BY if present
	if len(q.groupFields) > 0 {
		result += " GROUP BY "
		for i, field := range q.groupFields {
			if i > 0 {
				result += ", "
			}
			result += field
		}
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
