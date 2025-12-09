package metadata

import (
	"log/slog"
	"sync"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
)

// StatsManager manages statistical information for all tables
type StatsManager struct {
	tblMgr     *TableManager
	tableStats map[string]*StatInfo
	numCalls   int
	mutex      sync.RWMutex
}

// NewStatsManager creates a new StatsManager instance
func NewStatsManager(tblMgr *TableManager, tx *transaction.Transaction) *StatsManager {
	return &StatsManager{
		tblMgr:     tblMgr,
		tableStats: make(map[string]*StatInfo),
		numCalls:   0,
	}
}

// InvalidateStats clears cached stats for a specific table
func (sm *StatsManager) InvalidateStats(tblName string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()
	delete(sm.tableStats, tblName)
}

// GetStatInfo returns statistical information for a given table
func (sm *StatsManager) GetStatInfo(tblName string, layout *record.Layout, tx *transaction.Transaction) (*StatInfo, error) {
	slog.Info("GetStatInfo", "table", tblName)

	// Check cache with read lock first
	sm.mutex.RLock()
	si, exists := sm.tableStats[tblName]
	sm.mutex.RUnlock()

	// Increment call count and check if refresh is needed (but don't block on refresh)
	sm.mutex.Lock()
	sm.numCalls++
	shouldRefresh := sm.numCalls > 1000 && sm.numCalls%1000 == 0
	sm.mutex.Unlock()

	// If refresh is needed
	// Force recalculation of this table's stats
	if shouldRefresh {
		sm.mutex.Lock()
		sm.tableStats = make(map[string]*StatInfo)
		sm.mutex.Unlock()
		exists = false
	}

	// Need to calculate stats - acquire write lock to prevent concurrent calculations
	if !exists {
		sm.mutex.Lock()

		slog.Info("Recalculating stats", "table", tblName)
		calculated, err := sm.calcTableStats(tblName, layout, tx)
		if err != nil {
			sm.mutex.Unlock()
			return nil, err
		}
		sm.tableStats[tblName] = calculated
		si = calculated

		sm.mutex.Unlock()
	}

	return si, nil
}

// calcTableStats calculates statistics for a specific table by scanning all records
func (sm *StatsManager) calcTableStats(tblName string, layout *record.Layout, tx *transaction.Transaction) (*StatInfo, error) {
	numRecs := 0
	numBlocks := 0
	distinctVals := make(map[string]map[any]struct{})
	for _, field := range layout.GetSchema().Fields() {
		distinctVals[field] = make(map[any]struct{})
	}

	ts, err := table.NewTableScan(tx, layout, tblName)
	if err != nil {
		return nil, err
	}
	defer ts.Close()

	err = ts.BeforeFirst()
	if err != nil {
		return nil, err
	}

	for {
		hasNext, err := ts.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}

		// Track Number of Records
		numRecs++

		// Track Number of Blocks
		rid, err := ts.GetRID()
		if err != nil {
			continue
		}
		if rid.Block()+1 > numBlocks {
			numBlocks = rid.Block() + 1
		}

		// Track Distinct Values for each field
		for _, field := range layout.GetSchema().Fields() {
			val, err := ts.GetValue(field)
			if err != nil {
				return nil, err
			}
			distinctVals[field][val] = struct{}{}
		}
	}

	distinctCounts := make(map[string]int)
	for field, values := range distinctVals {
		distinctCounts[field] = len(values)
	}

	return NewStatInfo(numBlocks, numRecs, distinctCounts), nil
}
