package metadata

// StatInfo holds statistical information about a table
type StatInfo struct {
	numBlocks    int
	numRecs      int
	distinctVals map[string]int
}

// NewStatInfo creates a new StatInfo instance
func NewStatInfo(numBlocks, numRecs int, distinctVals map[string]int) *StatInfo {
	return &StatInfo{
		numBlocks:    numBlocks,
		numRecs:      numRecs,
		distinctVals: distinctVals,
	}
}

// BlocksAccessed returns the number of blocks accessed for this table
func (s *StatInfo) BlocksAccessed() int {
	return s.numBlocks
}

// RecordsOutput returns the number of records in this table
func (s *StatInfo) RecordsOutput() int {
	return s.numRecs
}

// DistinctValues returns the actual count of distinct values for a given field
func (s *StatInfo) DistinctValues(fieldName string) int {
	if _, exists := s.distinctVals[fieldName]; exists {
		return s.distinctVals[fieldName]
	}
	return 0
}
