package plan

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/temptable"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ Plan = (*SortPlan)(nil)
)

// SortPlan is the Plan for sorting records based on specified sort fields.
// It uses an external merge sort algorithm with temporary tables.
type SortPlan struct {
	p    Plan
	tx   *transaction.Transaction
	sch  *record.Schema
	comp *query.RecordComparator
}

// NewSortPlan creates a new SortPlan.
func NewSortPlan(p Plan, sortFields []string, tx *transaction.Transaction) *SortPlan {
	return &SortPlan{
		p:    p,
		tx:   tx,
		sch:  p.Schema(),
		comp: query.NewRecordComparator(sortFields),
	}
}

// Open performs the sorting operation and returns a sorted scan.
func (sp *SortPlan) Open() (scan.Scan, error) {
	src, err := sp.p.Open()
	if err != nil {
		return nil, err
	}

	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		src.Close()
		return nil, err
	}
	src.Close()

	if len(runs) == 0 {
		emptyTemp := temptable.NewTempTable(sp.tx, sp.sch)
		runs = []*temptable.TempTable{emptyTemp}
	}

	// Merge runs until we have at most 2 runs
	for len(runs) > 2 {
		runs, err = sp.doAMergeIteration(runs)
		if err != nil {
			return nil, err
		}
	}

	return query.NewSortScan(runs, sp.comp)
}

// BlocksAccessed returns the estimated number of blocks accessed.
// Uses MaterializePlan to estimate the cost (does not include one-time sorting cost).
func (sp *SortPlan) BlocksAccessed() int {
	mp := NewMaterializePlan(sp.tx, sp.p)
	return mp.BlocksAccessed()
}

// RecordsOutput returns the estimated number of output records.
func (sp *SortPlan) RecordsOutput() int {
	return sp.p.RecordsOutput()
}

// DistinctValues returns the estimated number of distinct values for a field.
func (sp *SortPlan) DistinctValues(fldname string) (int, error) {
	return sp.p.DistinctValues(fldname)
}

// Schema returns the schema of the output records.
func (sp *SortPlan) Schema() *record.Schema {
	return sp.sch
}

// Explain returns a string representation of the plan tree.
func (sp *SortPlan) Explain(indent string, lastChild bool) string {
	fieldsStr := "["
	for i, f := range sp.comp.Fields() {
		if i > 0 {
			fieldsStr += ", "
		}
		fieldsStr += f
	}
	fieldsStr += "]"
	result := indent + "SortPlan(fields: " + fieldsStr + ")\n"

	// Determine child indent
	childIndent := indent
	if lastChild {
		childIndent += "    "
	} else {
		childIndent += "│   "
	}
	childLines := sp.p.Explain(childIndent+"└─ ", true)
	result += childLines
	return result
}

// splitIntoRuns splits the source scan into sorted runs.
// Each run is a sorted sequence of records.
func (sp *SortPlan) splitIntoRuns(src scan.Scan) ([]*temptable.TempTable, error) {
	var temps []*temptable.TempTable

	err := src.BeforeFirst()
	if err != nil {
		return nil, err
	}

	hasNext, err := src.Next()
	if err != nil {
		return nil, err
	}
	if !hasNext {
		return temps, nil
	}

	// Create first temp table and scan
	currentTemp := temptable.NewTempTable(sp.tx, sp.sch)
	temps = append(temps, currentTemp)

	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}

	// Copy first record
	err = sp.copy(src, currentScan)
	if err != nil {
		currentScan.Close()
		return nil, err
	}

	// Continue copying records, starting new runs when needed
	for {
		hasNext, err := src.Next()
		if err != nil {
			currentScan.Close()
			return nil, err
		}
		if !hasNext {
			break
		}

		// Compare current record in source with last record in current run
		result, err := sp.comp.Compare(src, currentScan)
		if err != nil {
			currentScan.Close()
			return nil, err
		}

		if result < 0 {
			// Start a new run - current record is smaller than last in run
			currentScan.Close()
			currentTemp = temptable.NewTempTable(sp.tx, sp.sch)
			temps = append(temps, currentTemp)
			currentScan, err = currentTemp.Open()
			if err != nil {
				return nil, err
			}
		}

		// Copy the record
		err = sp.copy(src, currentScan)
		if err != nil {
			currentScan.Close()
			return nil, err
		}
	}

	currentScan.Close()
	return temps, nil
}

// doAMergeIteration merges pairs of runs in one iteration.
func (sp *SortPlan) doAMergeIteration(runs []*temptable.TempTable) ([]*temptable.TempTable, error) {
	var result []*temptable.TempTable

	for len(runs) > 1 {
		// Take first two runs
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]

		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil, err
		}
		result = append(result, merged)
	}

	// If there's one run left, add it to result
	if len(runs) == 1 {
		result = append(result, runs[0])
	}

	return result, nil
}

// mergeTwoRuns merges two sorted runs into a single sorted run.
func (sp *SortPlan) mergeTwoRuns(p1, p2 *temptable.TempTable) (*temptable.TempTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, err
	}
	defer src1.Close()

	src2, err := p2.Open()
	if err != nil {
		return nil, err
	}
	defer src2.Close()

	result := temptable.NewTempTable(sp.tx, sp.sch)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}
	defer dest.Close()

	err = src1.BeforeFirst()
	if err != nil {
		return nil, err
	}
	err = src2.BeforeFirst()
	if err != nil {
		return nil, err
	}

	hasMore1, err := src1.Next()
	if err != nil {
		return nil, err
	}
	hasMore2, err := src2.Next()
	if err != nil {
		return nil, err
	}

	// Merge while both have records
	for hasMore1 && hasMore2 {
		compareResult, err := sp.comp.Compare(src1, src2)
		if err != nil {
			return nil, err
		}

		if compareResult < 0 {
			// src1 is smaller
			err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
			hasMore1, err = src1.Next()
			if err != nil {
				return nil, err
			}
		} else {
			// src2 is smaller or equal
			err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
			hasMore2, err = src2.Next()
			if err != nil {
				return nil, err
			}
		}
	}

	// Copy remaining records from src1
	for hasMore1 {
		err = sp.copy(src1, dest)
		if err != nil {
			return nil, err
		}
		hasMore1, err = src1.Next()
		if err != nil {
			return nil, err
		}
	}

	// Copy remaining records from src2
	for hasMore2 {
		err = sp.copy(src2, dest)
		if err != nil {
			return nil, err
		}
		hasMore2, err = src2.Next()
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

// copy copies a record from source scan to destination scan.
// Returns true if there was a record to copy, false otherwise.
func (sp *SortPlan) copy(src scan.Scan, dest scan.UpdateScan) error {
	err := dest.Insert()
	if err != nil {
		return err
	}

	for _, fldname := range sp.sch.Fields() {
		val, err := src.GetValue(fldname)
		if err != nil {
			return err
		}

		if sp.sch.Type(fldname) == "int" {
			intVal, ok := val.(int)
			if !ok {
				return fmt.Errorf("expected int value for field %s, got %T", fldname, val)
			}
			err = dest.SetInt(fldname, intVal)
		} else {
			strVal, ok := val.(string)
			if !ok {
				return fmt.Errorf("expected string value for field %s, got %T", fldname, val)
			}
			err = dest.SetString(fldname, strVal)
		}
		if err != nil {
			return err
		}
	}

	return nil
}
