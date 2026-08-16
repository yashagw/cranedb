package plan

import (
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ Plan = (*HashJoinPlan)(nil)
)

// HashJoinPlan is the Plan for an in-memory hash join on an equi-join condition
// buildField = probeField. The hash table is built on whichever input has fewer
// estimated records, and the other input streams as the probe side.
type HashJoinPlan struct {
	build      Plan
	probe      Plan
	buildField string
	probeField string
	schema     *record.Schema
}

// NewHashJoinPlan creates a HashJoinPlan joining p1 and p2 on fld1 = fld2,
// where fld1 belongs to p1's schema and fld2 to p2's. The smaller input (by
// estimated record count) becomes the build side.
func NewHashJoinPlan(p1 Plan, p2 Plan, fld1 string, fld2 string) *HashJoinPlan {
	build, probe := p1, p2
	buildField, probeField := fld1, fld2
	if p2.RecordsOutput() < p1.RecordsOutput() {
		build, probe = p2, p1
		buildField, probeField = fld2, fld1
	}

	// Keep a stable p1-then-p2 field order regardless of which side builds.
	schema := record.NewSchema()
	schema.CopyAll(p1.Schema())
	schema.CopyAll(p2.Schema())

	return &HashJoinPlan{
		build:      build,
		probe:      probe,
		buildField: buildField,
		probeField: probeField,
		schema:     schema,
	}
}

// Open opens the build and probe plans and returns a HashJoinScan.
func (hjp *HashJoinPlan) Open() (scan.Scan, error) {
	buildScan, err := hjp.build.Open()
	if err != nil {
		return nil, err
	}

	probeScan, err := hjp.probe.Open()
	if err != nil {
		buildScan.Close()
		return nil, err
	}

	return query.NewHashJoinScan(buildScan, probeScan, hjp.buildField, hjp.probeField, hjp.build.Schema().Fields())
}

// BlocksAccessed estimates blocks accessed: each side is scanned exactly once.
// The in-memory hash table adds no I/O cost in this simple model.
func (hjp *HashJoinPlan) BlocksAccessed() int {
	return hjp.build.BlocksAccessed() + hjp.probe.BlocksAccessed()
}

// RecordsOutput estimates the equi-join output size:
// (build.records * probe.records) / max(V(build, buildField), V(probe, probeField)).
func (hjp *HashJoinPlan) RecordsOutput() int {
	maxDistinct := 1

	if v, err := hjp.build.DistinctValues(hjp.buildField); err == nil && v > maxDistinct {
		maxDistinct = v
	}
	if v, err := hjp.probe.DistinctValues(hjp.probeField); err == nil && v > maxDistinct {
		maxDistinct = v
	}

	return (hjp.build.RecordsOutput() * hjp.probe.RecordsOutput()) / maxDistinct
}

// DistinctValues delegates to whichever underlying plan contains the field.
func (hjp *HashJoinPlan) DistinctValues(fldname string) (int, error) {
	if hjp.build.Schema().HasField(fldname) {
		return hjp.build.DistinctValues(fldname)
	}
	return hjp.probe.DistinctValues(fldname)
}

// Schema returns the combined schema of both plans.
func (hjp *HashJoinPlan) Schema() *record.Schema {
	return hjp.schema
}

// Explain returns a string representation of the plan tree.
// The build side is printed as the first child so callers can tell which
// input was chosen to build the hash table.
func (hjp *HashJoinPlan) Explain(indent string, lastChild bool) string {
	result := indent + "HashJoinPlan(cond: " + hjp.buildField + " = " + hjp.probeField + ")\n"

	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}

	buildPrefix := childIndent + "├─ "
	result += hjp.build.Explain(buildPrefix, false) + "\n"

	probePrefix := childIndent + "└─ "
	result += hjp.probe.Explain(probePrefix, true)

	return result
}
