package parserdata

import "github.com/yashagw/cranedb/internal/query"

type DeleteData struct {
	table     string
	predicate *query.Predicate
}

func NewDeleteData(table string, predicate *query.Predicate) *DeleteData {
	return &DeleteData{
		table:     table,
		predicate: predicate,
	}
}

func (d *DeleteData) Table() string {
	return d.table
}

func (d *DeleteData) Predicate() *query.Predicate {
	return d.predicate
}
