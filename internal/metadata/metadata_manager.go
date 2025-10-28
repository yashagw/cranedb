package metadata

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

type Manager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	indexManager *IndexManager
	statsManager *StatsManager
}

func NewManager(isNew bool, tx *transaction.Transaction) *Manager {
	tableManager := NewTableManager(isNew, tx)
	viewManager := NewViewManager(isNew, tableManager, tx)
	indexManager := NewIndexManager(isNew, tableManager, NewStatsManager(tableManager, tx), tx)
	statsManager := NewStatsManager(tableManager, tx)

	return &Manager{
		tableManager: tableManager,
		viewManager:  viewManager,
		indexManager: indexManager,
		statsManager: statsManager,
	}
}

func (m *Manager) CreateTable(tableName string, schema *record.Schema, tx *transaction.Transaction) error {
	return m.tableManager.CreateTable(tableName, schema, tx)
}

func (m *Manager) CreateView(viewName string, viewDef string, tx *transaction.Transaction) error {
	return m.viewManager.CreateView(viewName, viewDef, tx)
}

func (m *Manager) CreateIndex(indexName string, tableName string, fieldName string, tx *transaction.Transaction) error {
	return m.indexManager.CreateIndex(indexName, tableName, fieldName, tx)
}

func (m *Manager) GetTableLayout(tableName string, tx *transaction.Transaction) (*record.Layout, error) {
	return m.tableManager.GetLayout(tableName, tx)
}

func (m *Manager) GetViewDef(viewName string, tx *transaction.Transaction) (string, error) {
	return m.viewManager.GetViewDef(viewName, tx)
}

func (m *Manager) GetIndexInfo(tableName string, tx *transaction.Transaction) (map[string]*IndexInfo, error) {
	return m.indexManager.GetIndexInfo(tableName, tx)
}

func (m *Manager) GetStatInfo(tableName string, layout *record.Layout, tx *transaction.Transaction) *StatInfo {
	return m.statsManager.GetStatInfo(tableName, layout, tx)
}
