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
	tablePlan, err := NewTablePlan(deleteData.Table(), tx, p.metadataManager)
	if err != nil {
		return 0, err
	}
	plan := NewSelectPlan(tablePlan, deleteData.Predicate())

	s, err := plan.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(scan.UpdateScan)
	if !ok {
		s.Close()
		return 0, nil
	}

	// Delete all matching records
	count := 0
	for {
		hasNext, err := us.Next()
		if err != nil {
			us.Close()
			return 0, err
		}
		if !hasNext {
			break
		}
		err = us.Delete()
		if err != nil {
			us.Close()
			return 0, err
		}
		count++
	}
	us.Close()

	return count, nil
}

// ExecuteModify executes an update statement and returns the number of records modified.
func (p *BasicUpdatePlanner) ExecuteModify(modifyData *parserdata.ModifyData, tx *transaction.Transaction) (int, error) {
	tablePlan, err := NewTablePlan(modifyData.Table(), tx, p.metadataManager)
	if err != nil {
		return 0, err
	}
	plan := NewSelectPlan(tablePlan, modifyData.Predicate())

	s, err := plan.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(scan.UpdateScan)
	if !ok {
		s.Close()
		return 0, nil
	}

	// Update all matching records
	count := 0
	for {
		hasNext, err := us.Next()
		if err != nil {
			us.Close()
			return 0, err
		}
		if !hasNext {
			break
		}
		val, err := modifyData.NewValue().Evaluate(us)
		if err != nil {
			us.Close()
			return 0, err
		}

		if val.IsInt() {
			err = us.SetInt(modifyData.FieldName(), val.AsInt())
			if err != nil {
				us.Close()
				return 0, err
			}
		} else {
			err = us.SetString(modifyData.FieldName(), val.AsString())
			if err != nil {
				us.Close()
				return 0, err
			}
		}

		count++
	}
	us.Close()

	return count, nil
}

// ExecuteInsert executes an insert statement and returns 1 (always inserts one record).
func (p *BasicUpdatePlanner) ExecuteInsert(insertData *parserdata.InsertData, tx *transaction.Transaction) (int, error) {
	plan, err := NewTablePlan(insertData.Table(), tx, p.metadataManager)
	if err != nil {
		return 0, err
	}

	s, err := plan.Open()
	if err != nil {
		return 0, err
	}
	us, ok := s.(scan.UpdateScan)
	if !ok {
		s.Close()
		return 0, nil
	}

	err = us.Insert()
	if err != nil {
		us.Close()
		return 0, err
	}

	rid, err := us.GetRID()
	if err != nil {
		us.Close()
		return 0, err
	}

	// Check if index exists for the table
	indexInfo, err := p.metadataManager.GetIndexInfo(insertData.Table(), tx)
	if err != nil {
		us.Close()
		return 0, err
	}

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

		if ii, exists := indexInfo[fieldName]; exists {
			index, err := ii.Open()
			if err != nil {
				us.Close()
				return 0, err
			}
			defer index.Close()
			err = index.Insert(val, rid)
			if err != nil {
				us.Close()
				return 0, err
			}
			err = index.Close()
			if err != nil {
				us.Close()
				return 0, err
			}
		}

		if constant != nil {
			if constant.IsInt() {
				err = us.SetInt(fieldName, constant.AsInt())
				if err != nil {
					us.Close()
					return 0, err
				}
			} else {
				err = us.SetString(fieldName, constant.AsString())
				if err != nil {
					us.Close()
					return 0, err
				}
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

// ExecuteCreateIndex executes a create index statement and returns 0.
func (p *BasicUpdatePlanner) ExecuteCreateIndex(createIndexData *parserdata.CreateIndexData, tx *transaction.Transaction) (int, error) {
	err := p.metadataManager.CreateIndex(createIndexData.IndexName(), createIndexData.TableName(), createIndexData.FieldName(), tx)
	if err != nil {
		return 0, err
	}
	return 0, nil
}
