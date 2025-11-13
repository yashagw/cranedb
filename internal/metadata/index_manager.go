package metadata

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
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

	ts, err := scan.NewTableScan(tx, layout, IndexCatalogName)
	if err != nil {
		return err
	}
	defer ts.Close()

	err = ts.Insert()
	if err != nil {
		return err
	}
	err = ts.SetString("indexname", indexName)
	if err != nil {
		return err
	}
	err = ts.SetString("tablename", tableName)
	if err != nil {
		return err
	}
	err = ts.SetString("fieldname", fieldName)
	if err != nil {
		return err
	}

	return nil
}

// GetIndexInfo returns map[fieldName]IndexInfo for all indexes on a table
func (im *IndexManager) GetIndexInfo(tableName string, tx *transaction.Transaction) (map[string]*IndexInfo, error) {
	layout, err := im.tableManager.GetLayout(IndexCatalogName, tx)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*IndexInfo)

	ts, err := scan.NewTableScan(tx, layout, IndexCatalogName)
	if err != nil {
		return nil, err
	}
	defer ts.Close()

	for {
		hasNext, err := ts.Next()
		if err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}
		tablenameVal, err := ts.GetString("tablename")
		if err != nil {
			return nil, err
		}
		if tablenameVal != tableName {
			continue
		}

		idxName, err := ts.GetString("indexname")
		if err != nil {
			return nil, err
		}
		fldName, err := ts.GetString("fieldname")
		if err != nil {
			return nil, err
		}

		tblLayout, err := im.tableManager.GetLayout(tableName, tx)
		if err != nil {
			return nil, err
		}
		si, err := im.statsManager.GetStatInfo(tableName, tblLayout, tx)
		if err != nil {
			return nil, err
		}
		ii := NewIndexInfo(idxName, fldName, tblLayout.GetSchema(), tx, si)

		result[fldName] = ii
	}

	return result, nil
}
