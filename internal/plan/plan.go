package plan

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

// Plan is the core interface for a relational algebra query plan node.
// It provides methods for both execution (Open) and query cost/metadata estimation.
type Plan interface {
	// Open returns a scan which can be used to iterate over the plan's records.
	Open() scan.Scan
	// BlocksAccessed returns the estimated number of blocks accessed by the operation.
	BlocksAccessed() int
	// RecordsOutput returns the estimated number of output records produced by this plan node.
	RecordsOutput() int
	// DistinctValues returns the estimated number of distinct values for a specified field in the output.
	DistinctValues(fldname string) int
	// Schema returns the schema of the output records produced by this plan node.
	Schema() *record.Schema
}
