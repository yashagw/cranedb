package metadata

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

const (
	ViewCatalogName = "view_catelog"
	MaxViewName     = 50
	MaxViewDef      = 100
)

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tableManager *TableManager, tx *transaction.Transaction) *ViewManager {
	vm := &ViewManager{
		tableManager: tableManager,
	}

	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("viewname", MaxViewName)
		schema.AddStringField("viewdef", MaxViewDef)
		tableManager.CreateTable(ViewCatalogName, schema, tx)
	}

	return vm
}

// CreateView creates a new view by inserting a record into the view catalog
func (v *ViewManager) CreateView(viewName string, viewDef string, tx *transaction.Transaction) error {
	layout, err := v.tableManager.GetLayout(ViewCatalogName, tx)
	if err != nil {
		return err
	}

	ts := record.NewTableScan(tx, layout, ViewCatalogName)
	defer ts.Close()

	ts.Insert()
	ts.SetString("viewname", viewName)
	ts.SetString("viewdef", viewDef)

	return nil
}

// GetViewDef retrieves the view definition for a given view name
func (v *ViewManager) GetViewDef(viewName string, tx *transaction.Transaction) (string, error) {
	layout, err := v.tableManager.GetLayout(ViewCatalogName, tx)
	if err != nil {
		return "", err
	}

	ts := record.NewTableScan(tx, layout, ViewCatalogName)
	defer ts.Close()

	for ts.Next() {
		if ts.GetString("viewname") == viewName {
			return ts.GetString("viewdef"), nil
		}
	}

	return "", nil
}
