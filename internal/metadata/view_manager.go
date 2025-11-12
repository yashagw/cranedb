package metadata

import (
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
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

	ts, err := scan.NewTableScan(tx, layout, ViewCatalogName)
	if err != nil {
		return err
	}
	defer ts.Close()

	err = ts.Insert()
	if err != nil {
		return err
	}
	err = ts.SetString("viewname", viewName)
	if err != nil {
		return err
	}
	err = ts.SetString("viewdef", viewDef)
	if err != nil {
		return err
	}

	return nil
}

// GetViewDef retrieves the view definition for a given view name
func (v *ViewManager) GetViewDef(viewName string, tx *transaction.Transaction) (string, error) {
	layout, err := v.tableManager.GetLayout(ViewCatalogName, tx)
	if err != nil {
		return "", err
	}

	ts, err := scan.NewTableScan(tx, layout, ViewCatalogName)
	if err != nil {
		return "", err
	}
	defer ts.Close()

	for {
		hasNext, err := ts.Next()
		if err != nil {
			return "", err
		}
		if !hasNext {
			break
		}
		viewnameVal, err := ts.GetString("viewname")
		if err != nil {
			continue
		}
		if viewnameVal == viewName {
			viewdefVal, err := ts.GetString("viewdef")
			if err != nil {
				return "", err
			}
			return viewdefVal, nil
		}
	}

	return "", nil
}
