package metadata

import (
	"fmt"
	"sync"

	"github.com/yashagw/cranedb/internal/record"
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
func (s *StatInfo) DistinctValues(fieldName string, tx *transaction.Transaction, tableName string) int {
	s.mutex.RLock()
	if cached, exists := s.distinctVals[fieldName]; exists {
		s.mutex.RUnlock()
		return cached
	}
	s.mutex.RUnlock()

	distinctCount := s.calculateActualDistinctValues(fieldName, tx, tableName)

	s.mutex.Lock()
	s.distinctVals[fieldName] = distinctCount
	s.mutex.Unlock()

	return distinctCount
}

// calculateActualDistinctValues scans through all records to count distinct values
func (s *StatInfo) calculateActualDistinctValues(fieldName string, tx *transaction.Transaction, tableName string) int {
	if s.numRecs == 0 {
		return 0
	}

	if s.layout == nil {
		return 1
	}

	schema := s.layout.GetSchema()
	if schema == nil {
		return 1
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
		return 0
	}

	distinctValues := make(map[string]bool)

	ts := record.NewTableScan(tx, s.layout, tableName)
	defer ts.Close()

	for ts.Next() {
		fieldType := schema.Type(fieldName)
		var value string

		if fieldType == "int" {
			intValue := ts.GetInt(fieldName)
			value = fmt.Sprintf("%d", intValue)
		} else if fieldType == "string" {
			value = ts.GetString(fieldName)
		} else {
			continue
		}

		distinctValues[value] = true
	}

	return len(distinctValues)
}
