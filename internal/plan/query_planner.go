package plan

import (
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ QueryPlanner = (*BasicQueryPlanner)(nil)
)

type BasicQueryPlanner struct {
	metadataManager *metadata.Manager
}

func NewBasicQueryPlanner(metadataManager *metadata.Manager) *BasicQueryPlanner {
	return &BasicQueryPlanner{
		metadataManager: metadataManager,
	}
}

func (p *BasicQueryPlanner) CreatePlan(queryData *parserdata.QueryData, tx *transaction.Transaction) (Plan, error) {
	// Create a plan for each table in the query
	tablePlans := make([]Plan, len(queryData.Tables()))
	for i, tableName := range queryData.Tables() {
		tablePlan, err := NewTablePlan(tableName, tx, p.metadataManager)
		if err != nil {
			return nil, err
		}
		tablePlans[i] = tablePlan
	}

	// Create the product of all table plans
	plan := tablePlans[0]
	for i := 1; i < len(tablePlans); i++ {
		p1 := NewProductPlan(plan, tablePlans[i])
		p2 := NewProductPlan(tablePlans[i], plan)

		if p1.BlocksAccessed() < p2.BlocksAccessed() {
			plan = p1
		} else {
			plan = p2
		}
	}

	// Apply the predicate to the plan (only if predicate exists)
	if queryData.Predicate() != nil {
		plan = NewSelectPlan(plan, queryData.Predicate())
	}

	// Project the fields from the plan
	plan = NewProjectPlan(plan, queryData.Fields())

	return plan, nil
}
