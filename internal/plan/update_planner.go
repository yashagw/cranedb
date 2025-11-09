package plan

import (
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ UpdatePlanner = (*BasicUpdatePlanner)(nil)
)

type BasicUpdatePlanner struct {
	metadataManager *metadata.Manager
}

func NewBasicUpdatePlanner(metadataManager *metadata.Manager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		metadataManager: metadataManager,
	}
}

// ExecuteDelete executes a delete statement and returns the number of records deleted.
func (p *BasicUpdatePlanner) ExecuteDelete(deleteData *parserdata.DeleteData, tx *transaction.Transaction) (int, error) {
	tablePlan := NewTablePlan(deleteData.Table(), tx, p.metadataManager)
	plan := NewSelectPlan(tablePlan, deleteData.Predicate())

	s := plan.Open()
	us, ok := s.(scan.UpdateScan)
	if !ok {
		s.Close()
		return 0, nil
	}

	// Delete all matching records
	count := 0
	for us.Next() {
		us.Delete()
		count++
	}
	us.Close()

	return count, nil
}

// ExecuteModify executes an update statement and returns the number of records modified.
func (p *BasicUpdatePlanner) ExecuteModify(modifyData *parserdata.ModifyData, tx *transaction.Transaction) (int, error) {
	tablePlan := NewTablePlan(modifyData.Table(), tx, p.metadataManager)
	plan := NewSelectPlan(tablePlan, modifyData.Predicate())

	s := plan.Open()
	us, ok := s.(scan.UpdateScan)
	if !ok {
		s.Close()
		return 0, nil
	}

	// Update all matching records
	count := 0
	for us.Next() {
		val := modifyData.NewValue().Evaluate(us)

		if val.IsInt() {
			us.SetInt(modifyData.FieldName(), val.AsInt())
		} else {
			us.SetString(modifyData.FieldName(), val.AsString())
		}

		count++
	}
	us.Close()

	return count, nil
}

// ExecuteInsert executes an insert statement and returns 1 (always inserts one record).
func (p *BasicUpdatePlanner) ExecuteInsert(insertData *parserdata.InsertData, tx *transaction.Transaction) (int, error) {
	plan := NewTablePlan(insertData.Table(), tx, p.metadataManager)

	s := plan.Open()
	us, ok := s.(scan.UpdateScan)
	if !ok {
		s.Close()
		return 0, nil
	}

	us.Insert()

	fields := insertData.Fields()
	values := insertData.Values()

	// Set field values
	for i, fieldName := range fields {
		val := values[i]

		var constant *query.Constant
		switch v := val.(type) {
		case int:
			constant = query.NewIntConstant(v)
		case string:
			constant = query.NewStringConstant(v)
		case *query.Constant:
			constant = v
		case query.Constant:
			constant = &v
		}

		if constant != nil {
			if constant.IsInt() {
				us.SetInt(fieldName, constant.AsInt())
			} else {
				us.SetString(fieldName, constant.AsString())
			}
		}
	}

	us.Close()
	return 1, nil
}

// ExecuteCreateTable executes a create table statement and returns 0.
func (p *BasicUpdatePlanner) ExecuteCreateTable(createTableData *parserdata.CreateTableData, tx *transaction.Transaction) (int, error) {
	err := p.metadataManager.CreateTable(createTableData.TableName(), createTableData.Schema(), tx)
	if err != nil {
		return 0, err
	}
	return 0, nil
}

// ExecuteCreateView executes a create view statement and returns 0.
func (p *BasicUpdatePlanner) ExecuteCreateView(createViewData *parserdata.CreateViewData, tx *transaction.Transaction) (int, error) {
	err := p.metadataManager.CreateView(createViewData.ViewName(), createViewData.Query().String(), tx)
	if err != nil {
		return 0, err
	}
	return 0, nil
}
