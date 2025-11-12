package metadata

import (
	"log"
	"sync"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
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
	log.Printf("[STATS] GetStatInfo: Starting for table %s", tblName)

	// Check cache with read lock first
	sm.mutex.RLock()
	si, exists := sm.tableStats[tblName]
	sm.mutex.RUnlock()

	// Increment call count and check if refresh is needed (but don't block on refresh)
	sm.mutex.Lock()
	sm.numCalls++
	shouldRefresh := sm.numCalls > 100 && sm.numCalls%100 == 0 // Only refresh every 100 calls, not every call after 100
	sm.mutex.Unlock()

	// If refresh is needed, do it asynchronously to avoid blocking
	if shouldRefresh {
		log.Printf("[STATS] GetStatInfo: Scheduling async refresh (numCalls=%d)", sm.numCalls)
		// Don't block - just clear the cache and let stats be recalculated on demand
		// This is much better than scanning all tables synchronously
		sm.mutex.Lock()
		sm.tableStats = make(map[string]*StatInfo)
		sm.numCalls = 0
		sm.mutex.Unlock()
		log.Printf("[STATS] GetStatInfo: Cleared cache for lazy refresh")
		exists = false // Force recalculation of this table's stats
	}

	if !exists {
		// Need to calculate stats - acquire write lock to prevent concurrent calculations
		log.Printf("[STATS] GetStatInfo: Stats not cached for %s, acquiring lock to calculate...", tblName)
		sm.mutex.Lock()
		// Double-check after acquiring lock (another goroutine might have calculated it)
		si, exists = sm.tableStats[tblName]
		if !exists {
			log.Printf("[STATS] GetStatInfo: Calculating stats for %s (holding lock)", tblName)
			si = sm.calcTableStats(tblName, layout, tx)
			log.Printf("[STATS] GetStatInfo: Finished calculating stats for %s (blocks=%d, recs=%d)", tblName, si.numBlocks, si.numRecs)
			sm.tableStats[tblName] = si
		} else {
			log.Printf("[STATS] GetStatInfo: Stats were calculated by another goroutine while waiting for lock")
		}
		sm.mutex.Unlock()
	} else {
		log.Printf("[STATS] GetStatInfo: Using cached stats for %s", tblName)
	}

	return si
}

// GetDistinctValues is a convenience method that gets distinct values for a field
func (sm *StatsManager) GetDistinctValues(tblName string, fieldName string, layout *record.Layout, tx *transaction.Transaction) (int, error) {
	si := sm.GetStatInfo(tblName, layout, tx)
	return si.DistinctValues(fieldName, tx, tblName)
}

// calcTableStats calculates statistics for a specific table by scanning all records
func (sm *StatsManager) calcTableStats(tblName string, layout *record.Layout, tx *transaction.Transaction) *StatInfo {
	log.Printf("[STATS] calcTableStats: Starting scan of table %s", tblName)
	numRecs := 0
	numBlocks := 0

	ts, err := scan.NewTableScan(tx, layout, tblName)
	if err != nil {
		log.Printf("[STATS] calcTableStats: NewTableScan failed for %s: %v", tblName, err)
		return NewStatInfo(0, 0, layout)
	}
	defer ts.Close()
	log.Printf("[STATS] calcTableStats: Opened scan for %s, starting iteration", tblName)

	iterations := 0
	for {
		iterations++
		if iterations%10 == 0 {
			log.Printf("[STATS] calcTableStats: Scanned %d records in %s", numRecs, tblName)
		}
		hasNext, err := ts.Next()
		if err != nil {
			log.Printf("[STATS] calcTableStats: Next() failed for %s: %v", tblName, err)
			return NewStatInfo(numBlocks, numRecs, layout)
		}
		if !hasNext {
			break
		}
		numRecs++
		rid, err := ts.GetRID()
		if err != nil {
			continue
		}
		if rid.Block()+1 > numBlocks {
			numBlocks = rid.Block() + 1
		}
	}

	log.Printf("[STATS] calcTableStats: Completed scan of %s: %d records, %d blocks", tblName, numRecs, numBlocks)
	return NewStatInfo(numBlocks, numRecs, layout)
}
