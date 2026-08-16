package query

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ scan.Scan = (*HashJoinScan)(nil)
)

// buildRow is one buffered row from the build-side scan.
type buildRow struct {
	key    Constant       // join-key value, verified with Equals at probe time
	values map[string]any // build-side field name -> raw value
}

// HashJoinScan implements an in-memory hash join.
// It drains the build-side scan into a hash table keyed by the join field,
// then streams the probe side, emitting one row per matching build row.
type HashJoinScan struct {
	build      scan.Scan
	probe      scan.Scan
	buildField string
	probeField string

	buildExpr     *Expression
	probeExpr     *Expression
	buildFieldSet map[string]struct{}

	// Constant.Hash() -> chain of rows; duplicates and hash collisions share
	// a bucket and are disambiguated by key.Equals during probing.
	table map[int][]buildRow
	built bool

	probeKey   Constant
	candidates []buildRow
	candIdx    int
	current    *buildRow
}

// NewHashJoinScan creates a new HashJoinScan joining build.buildField = probe.probeField.
// buildFields lists the field names to buffer from the build-side scan.
func NewHashJoinScan(build, probe scan.Scan, buildField, probeField string, buildFields []string) (*HashJoinScan, error) {
	fieldSet := make(map[string]struct{}, len(buildFields))
	for _, f := range buildFields {
		fieldSet[f] = struct{}{}
	}

	hjs := &HashJoinScan{
		build:         build,
		probe:         probe,
		buildField:    buildField,
		probeField:    probeField,
		buildExpr:     NewFieldNameExpression(buildField),
		probeExpr:     NewFieldNameExpression(probeField),
		buildFieldSet: fieldSet,
	}
	if err := hjs.BeforeFirst(); err != nil {
		return nil, err
	}
	return hjs, nil
}

// BeforeFirst positions the scan before the first record.
// The hash table is built once and reused across re-scans, since the build
// input is fixed for the lifetime of the scan.
func (hjs *HashJoinScan) BeforeFirst() error {
	if !hjs.built {
		if err := hjs.buildTable(); err != nil {
			return err
		}
		hjs.built = true
	}
	if err := hjs.probe.BeforeFirst(); err != nil {
		return err
	}
	hjs.candidates = nil
	hjs.candIdx = -1
	hjs.current = nil
	return nil
}

// buildTable drains the build-side scan into the in-memory hash table.
func (hjs *HashJoinScan) buildTable() error {
	hjs.table = make(map[int][]buildRow)

	if err := hjs.build.BeforeFirst(); err != nil {
		return err
	}
	for {
		hasNext, err := hjs.build.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			return nil
		}

		key, err := hjs.buildExpr.Evaluate(hjs.build)
		if err != nil {
			return err
		}

		values := make(map[string]any, len(hjs.buildFieldSet))
		for f := range hjs.buildFieldSet {
			val, err := hjs.build.GetValue(f)
			if err != nil {
				return err
			}
			values[f] = val
		}

		h := key.Hash()
		hjs.table[h] = append(hjs.table[h], buildRow{key: key, values: values})
	}
}

// Next moves to the next joined record.
// It walks the current probe row's bucket; when exhausted, it advances the
// probe scan and loads the bucket for the new probe key.
func (hjs *HashJoinScan) Next() (bool, error) {
	for {
		for hjs.candIdx+1 < len(hjs.candidates) {
			hjs.candIdx++
			if hjs.candidates[hjs.candIdx].key.Equals(&hjs.probeKey) {
				hjs.current = &hjs.candidates[hjs.candIdx]
				return true, nil
			}
		}

		hasNext, err := hjs.probe.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			hjs.current = nil
			return false, nil
		}

		key, err := hjs.probeExpr.Evaluate(hjs.probe)
		if err != nil {
			return false, err
		}
		hjs.probeKey = key
		hjs.candidates = hjs.table[key.Hash()]
		hjs.candIdx = -1
	}
}

// GetInt returns the integer value of the specified field.
// It checks the buffered build side first, then the probe side.
func (hjs *HashJoinScan) GetInt(fldname string) (int, error) {
	if _, ok := hjs.buildFieldSet[fldname]; ok {
		val, err := hjs.buildValue(fldname)
		if err != nil {
			return 0, err
		}
		intVal, ok := val.(int)
		if !ok {
			return 0, fmt.Errorf("field %s is not an int", fldname)
		}
		return intVal, nil
	}
	return hjs.probe.GetInt(fldname)
}

// GetBool returns the boolean value of the specified field.
// It checks the buffered build side first, then the probe side.
func (hjs *HashJoinScan) GetBool(fldname string) (bool, error) {
	if _, ok := hjs.buildFieldSet[fldname]; ok {
		val, err := hjs.buildValue(fldname)
		if err != nil {
			return false, err
		}
		boolVal, ok := val.(bool)
		if !ok {
			return false, fmt.Errorf("field %s is not a bool", fldname)
		}
		return boolVal, nil
	}
	return hjs.probe.GetBool(fldname)
}

// GetString returns the string value of the specified field.
// It checks the buffered build side first, then the probe side.
func (hjs *HashJoinScan) GetString(fldname string) (string, error) {
	if _, ok := hjs.buildFieldSet[fldname]; ok {
		val, err := hjs.buildValue(fldname)
		if err != nil {
			return "", err
		}
		strVal, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("field %s is not a string", fldname)
		}
		return strVal, nil
	}
	return hjs.probe.GetString(fldname)
}

// GetValue returns the value of the specified field.
// It checks the buffered build side first, then the probe side.
func (hjs *HashJoinScan) GetValue(fldname string) (any, error) {
	if _, ok := hjs.buildFieldSet[fldname]; ok {
		return hjs.buildValue(fldname)
	}
	return hjs.probe.GetValue(fldname)
}

// HasField checks if the scan contains the specified field.
func (hjs *HashJoinScan) HasField(fldname string) bool {
	if _, ok := hjs.buildFieldSet[fldname]; ok {
		return true
	}
	return hjs.probe.HasField(fldname)
}

// Close closes both underlying scans and releases the hash table.
func (hjs *HashJoinScan) Close() {
	hjs.build.Close()
	hjs.probe.Close()
	hjs.table = nil
	hjs.candidates = nil
	hjs.current = nil
}

// buildValue returns the buffered build-side value for the current joined record.
func (hjs *HashJoinScan) buildValue(fldname string) (any, error) {
	if hjs.current == nil {
		return nil, fmt.Errorf("no current record in hash join scan")
	}
	return hjs.current.values[fldname], nil
}
