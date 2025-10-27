package metadata

import (
	"sync"

	"github.com/yashagw/cranedb/internal/record"
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

// GetStatInfo returns statistical information for a given table
func (sm *StatsManager) GetStatInfo(tblName string, layout *record.Layout, tx *transaction.Transaction) *StatInfo {
	sm.mutex.Lock()
	sm.numCalls++
	if sm.numCalls > 100 {
		sm.refreshStatistics(tx)
	}
	sm.mutex.Unlock()

	sm.mutex.RLock()
	si, exists := sm.tableStats[tblName]
	sm.mutex.RUnlock()

	if !exists {
		si = sm.calcTableStats(tblName, layout, tx)
		sm.mutex.Lock()
		sm.tableStats[tblName] = si
		sm.mutex.Unlock()
	}

	return si
}

// GetDistinctValues is a convenience method that gets distinct values for a field
func (sm *StatsManager) GetDistinctValues(tblName string, fieldName string, layout *record.Layout, tx *transaction.Transaction) int {
	si := sm.GetStatInfo(tblName, layout, tx)
	return si.DistinctValues(fieldName, tx, tblName)
}

// refreshStatistics refreshes all table statistics by scanning the table catalog
func (sm *StatsManager) refreshStatistics(tx *transaction.Transaction) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.tableStats = make(map[string]*StatInfo)
	sm.numCalls = 0

	layout, err := sm.tblMgr.GetLayout(TableCatalogName, tx)
	if err != nil {
		return
	}

	tcat := record.NewTableScan(tx, layout, TableCatalogName)
	defer tcat.Close()

	for tcat.Next() {
		tblName := tcat.GetString("table_name")
		tableLayout, err := sm.tblMgr.GetLayout(tblName, tx)
		if err != nil {
			continue
		}

		si := sm.calcTableStats(tblName, tableLayout, tx)
		sm.tableStats[tblName] = si
	}
}

// calcTableStats calculates statistics for a specific table by scanning all records
func (sm *StatsManager) calcTableStats(tblName string, layout *record.Layout, tx *transaction.Transaction) *StatInfo {
	numRecs := 0
	numBlocks := 0

	ts := record.NewTableScan(tx, layout, tblName)
	defer ts.Close()

	for ts.Next() {
		numRecs++
		rid := ts.GetRID()
		if rid.Block()+1 > numBlocks {
			numBlocks = rid.Block() + 1
		}
	}

	return NewStatInfo(numBlocks, numRecs, layout)
}
