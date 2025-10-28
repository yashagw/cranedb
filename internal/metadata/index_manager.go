package metadata

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

const (
	IndexCatalogName = "idx_catelog"
	MaxIndexName     = 50
)

type IndexManager struct {
	tableManager *TableManager
	statsManager *StatsManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statsManager *StatsManager, tx *transaction.Transaction) *IndexManager {
	im := &IndexManager{
		tableManager: tableManager,
		statsManager: statsManager,
	}

	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("indexname", MaxIndexName)
		schema.AddStringField("tablename", MaxStringSize)
		schema.AddStringField("fieldname", MaxStringSize)
		tableManager.CreateTable(IndexCatalogName, schema, tx)
	}

	return im
}

// CreateIndex inserts a new index metadata row into the index catalog
func (im *IndexManager) CreateIndex(indexName string, tableName string, fieldName string, tx *transaction.Transaction) error {
	layout, err := im.tableManager.GetLayout(IndexCatalogName, tx)
	if err != nil {
		return err
	}

	ts := record.NewTableScan(tx, layout, IndexCatalogName)
	defer ts.Close()

	ts.Insert()
	ts.SetString("indexname", indexName)
	ts.SetString("tablename", tableName)
	ts.SetString("fieldname", fieldName)

	return nil
}

// GetIndexInfo returns map[fieldName]IndexInfo for all indexes on a table
func (im *IndexManager) GetIndexInfo(tableName string, tx *transaction.Transaction) (map[string]*IndexInfo, error) {
	layout, err := im.tableManager.GetLayout(IndexCatalogName, tx)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*IndexInfo)

	ts := record.NewTableScan(tx, layout, IndexCatalogName)
	defer ts.Close()

	for ts.Next() {
		if ts.GetString("tablename") != tableName {
			continue
		}

		idxName := ts.GetString("indexname")
		fldName := ts.GetString("fieldname")

		tblLayout, err := im.tableManager.GetLayout(tableName, tx)
		if err != nil {
			return nil, err
		}
		si := im.statsManager.GetStatInfo(tableName, tblLayout, tx)

		ii := &IndexInfo{
			indexName:   idxName,
			fieldName:   fldName,
			tableSchema: tblLayout.GetSchema(),
			indexLayout: createIndexLayout(tblLayout, fldName),
			stats:       si,
		}
		result[fldName] = ii
	}

	return result, nil
}
