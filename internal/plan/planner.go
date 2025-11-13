package plan

import (
	"errors"

	"github.com/yashagw/cranedb/internal/parse"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/transaction"
)

type QueryPlanner interface {
	CreatePlan(queryData *parserdata.QueryData, tx *transaction.Transaction) (Plan, error)
}

type UpdatePlanner interface {
	ExecuteModify(modifyData *parserdata.ModifyData, tx *transaction.Transaction) (int, error)
	ExecuteInsert(insertData *parserdata.InsertData, tx *transaction.Transaction) (int, error)
	ExecuteDelete(deleteData *parserdata.DeleteData, tx *transaction.Transaction) (int, error)
	ExecuteCreateTable(createTableData *parserdata.CreateTableData, tx *transaction.Transaction) (int, error)
	ExecuteCreateView(createViewData *parserdata.CreateViewData, tx *transaction.Transaction) (int, error)
	ExecuteCreateIndex(createIndexData *parserdata.CreateIndexData, tx *transaction.Transaction) (int, error)
}

type Planner struct {
	queryPlanner  QueryPlanner
	updatePlanner UpdatePlanner
}

func NewPlanner(queryPlanner QueryPlanner, updatePlanner UpdatePlanner) *Planner {
	return &Planner{
		queryPlanner:  queryPlanner,
		updatePlanner: updatePlanner,
	}
}

func (p *Planner) CreatePlan(sql string, tx *transaction.Transaction) (Plan, error) {
	parser := parse.NewParserFromString(sql)
	queryData, err := parser.Query()
	if err != nil {
		return nil, err
	}
	return p.queryPlanner.CreatePlan(queryData, tx)
}

func (p *Planner) ExecuteUpdate(sql string, tx *transaction.Transaction) (int, error) {
	parser := parse.NewParserFromString(sql)
	updateData, err := parser.UpdateCmd()
	if err != nil {
		return 0, err
	}

	switch updateData := updateData.(type) {
	case *parserdata.ModifyData:
		return p.updatePlanner.ExecuteModify(updateData, tx)
	case *parserdata.InsertData:
		return p.updatePlanner.ExecuteInsert(updateData, tx)
	case *parserdata.DeleteData:
		return p.updatePlanner.ExecuteDelete(updateData, tx)
	case *parserdata.CreateTableData:
		return p.updatePlanner.ExecuteCreateTable(updateData, tx)
	case *parserdata.CreateViewData:
		return p.updatePlanner.ExecuteCreateView(updateData, tx)
	case *parserdata.CreateIndexData:
		return p.updatePlanner.ExecuteCreateIndex(updateData, tx)
	}

	return 0, errors.New("invalid update command")
}
