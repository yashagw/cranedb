package parserdata

// ExplainData wraps a QueryData for EXPLAIN statements
type ExplainData struct {
	queryData *QueryData
}

// NewExplainData creates a new ExplainData
func NewExplainData(queryData *QueryData) *ExplainData {
	return &ExplainData{
		queryData: queryData,
	}
}

// QueryData returns the underlying query data
func (e *ExplainData) QueryData() *QueryData {
	return e.queryData
}
