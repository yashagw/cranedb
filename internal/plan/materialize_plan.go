package plan

import (
	"fmt"
	"math"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/temptable"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ Plan = (*MaterializePlan)(nil)
)

// MaterializePlan is the Plan for materializing a query result into a temporary table.
// This is useful for queries that need to be scanned multiple times or for optimization.
type MaterializePlan struct {
	srcPlan Plan
	tx      *transaction.Transaction
}

// NewMaterializePlan creates a new MaterializePlan.
func NewMaterializePlan(tx *transaction.Transaction, srcPlan Plan) *MaterializePlan {
	return &MaterializePlan{
		srcPlan: srcPlan,
		tx:      tx,
	}
}

// Open materializes the source plan's results into a temporary table and returns a scan on it.
func (mp *MaterializePlan) Open() (scan.Scan, error) {
	sch := mp.srcPlan.Schema()
	temp := temptable.NewTempTable(mp.tx, sch)

	src, err := mp.srcPlan.Open()
	if err != nil {
		return nil, err
	}

	dest, err := temp.Open()
	if err != nil {
		src.Close()
		return nil, err
	}

	// Copy all records from source to destination
	for {
		hasNext, err := src.Next()
		if err != nil {
			src.Close()
			dest.Close()
			return nil, err
		}
		if !hasNext {
			break
		}

		err = dest.Insert()
		if err != nil {
			src.Close()
			dest.Close()
			return nil, err
		}

		for _, fldname := range sch.Fields() {
			val, err := src.GetValue(fldname)
			if err != nil {
				src.Close()
				dest.Close()
				return nil, err
			}

			if sch.Type(fldname) == "int" {
				intVal, ok := val.(int)
				if !ok {
					src.Close()
					dest.Close()
					return nil, fmt.Errorf("expected int value for field %s, got %T", fldname, val)
				}
				err = dest.SetInt(fldname, intVal)
			} else {
				strVal, ok := val.(string)
				if !ok {
					src.Close()
					dest.Close()
					return nil, fmt.Errorf("expected string value for field %s, got %T", fldname, val)
				}
				err = dest.SetString(fldname, strVal)
			}
			if err != nil {
				src.Close()
				dest.Close()
				return nil, err
			}
		}
	}

	src.Close()
	err = dest.BeforeFirst()
	if err != nil {
		dest.Close()
		return nil, err
	}

	return dest, nil
}

// BlocksAccessed returns the estimated number of blocks accessed.
// Formula: ceil(recordsOutput / (blockSize / slotSize))
func (mp *MaterializePlan) BlocksAccessed() int {
	// Create a layout object to calculate slot size
	layout := record.NewLayoutFromSchema(mp.srcPlan.Schema())
	blockSize := mp.tx.BlockSize()
	slotSize := layout.GetSlotSize()
	rpb := float64(blockSize) / float64(slotSize)
	return int(math.Ceil(float64(mp.srcPlan.RecordsOutput()) / rpb))
}

// RecordsOutput returns the estimated number of output records.
func (mp *MaterializePlan) RecordsOutput() int {
	return mp.srcPlan.RecordsOutput()
}

// DistinctValues returns the estimated number of distinct values for a field.
func (mp *MaterializePlan) DistinctValues(fldname string) (int, error) {
	return mp.srcPlan.DistinctValues(fldname)
}

// Schema returns the schema of the output records.
func (mp *MaterializePlan) Schema() *record.Schema {
	return mp.srcPlan.Schema()
}

// Explain returns a string representation of the plan tree.
func (mp *MaterializePlan) Explain(indent string, lastChild bool) string {
	result := indent + "MaterializePlan\n"

	// Determine child indent
	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}
	childLines := mp.srcPlan.Explain(childIndent+"└─ ", true)
	result += childLines
	return result
}
