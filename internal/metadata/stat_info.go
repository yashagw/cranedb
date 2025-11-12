package metadata

import (
	"fmt"
	"sync"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/transaction"
)

// StatInfo holds statistical information about a table
type StatInfo struct {
	numBlocks    int
	numRecs      int
	distinctVals map[string]int
	layout       *record.Layout
	mutex        sync.RWMutex
}

// NewStatInfo creates a new StatInfo instance
func NewStatInfo(numBlocks, numRecs int, layout *record.Layout) *StatInfo {
	return &StatInfo{
		numBlocks:    numBlocks,
		numRecs:      numRecs,
		distinctVals: make(map[string]int),
		layout:       layout,
	}
}

// BlocksAccessed returns the number of blocks accessed for this table
func (s *StatInfo) BlocksAccessed() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.numBlocks
}

// RecordsOutput returns the number of records in this table
func (s *StatInfo) RecordsOutput() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.numRecs
}

// DistinctValues returns the actual count of distinct values for a given field
func (s *StatInfo) DistinctValues(fieldName string, tx *transaction.Transaction, tableName string) (int, error) {
	s.mutex.RLock()
	if cached, exists := s.distinctVals[fieldName]; exists {
		s.mutex.RUnlock()
		return cached, nil
	}
	s.mutex.RUnlock()

	distinctCount, err := s.calculateActualDistinctValues(fieldName, tx, tableName)
	if err != nil {
		return 0, err
	}

	s.mutex.Lock()
	s.distinctVals[fieldName] = distinctCount
	s.mutex.Unlock()

	return distinctCount, nil
}

// calculateActualDistinctValues scans through all records to count distinct values
func (s *StatInfo) calculateActualDistinctValues(fieldName string, tx *transaction.Transaction, tableName string) (int, error) {
	if s.numRecs == 0 {
		// Empty table has 0 distinct values
		return 0, nil
	}

	if s.layout == nil {
		return 0, fmt.Errorf("cannot calculate distinct values: layout is nil")
	}

	schema := s.layout.GetSchema()
	if schema == nil {
		return 0, fmt.Errorf("cannot calculate distinct values: schema is nil")
	}

	fields := schema.Fields()
	fieldExists := false
	for _, field := range fields {
		if field == fieldName {
			fieldExists = true
			break
		}
	}

	if !fieldExists {
		return 0, fmt.Errorf("cannot calculate distinct values: field '%s' does not exist", fieldName)
	}

	distinctValues := make(map[string]bool)

	ts, err := scan.NewTableScan(tx, s.layout, tableName)
	if err != nil {
		return 0, fmt.Errorf("failed to create table scan: %w", err)
	}
	defer ts.Close()

	err = ts.BeforeFirst()
	if err != nil {
		return 0, fmt.Errorf("failed to initialize table scan: %w", err)
	}

	for {
		hasNext, err := ts.Next()
		if err != nil {
			return 0, fmt.Errorf("failed to read next record: %w", err)
		}
		if !hasNext {
			break
		}
		fieldType := schema.Type(fieldName)
		var value string

		if fieldType == "int" {
			intValue, err := ts.GetInt(fieldName)
			if err != nil {
				continue
			}
			value = fmt.Sprintf("%d", intValue)
		} else if fieldType == "string" {
			strValue, err := ts.GetString(fieldName)
			if err != nil {
				continue
			}
			value = strValue
		} else {
			continue
		}

		distinctValues[value] = true
	}

	return len(distinctValues), nil
}
