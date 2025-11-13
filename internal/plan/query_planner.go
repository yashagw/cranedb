package plan

import (
	"sort"

	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
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
	tables := queryData.Tables()
	predicate := queryData.Predicate()

	// Phase 1: Create optimized table plans with index selection
	tablePlans := make([]Plan, len(tables))
	for i, tableName := range tables {
		tablePlan, err := NewTablePlan(tableName, tx, p.metadataManager)
		if err != nil {
			return nil, err
		}

		// Apply index optimization for this table
		if predicate != nil {
			optimizedPlan, err := p.optimizeTableWithIndex(tablePlan, tableName, predicate, tx)
			if err != nil {
				return nil, err
			}
			tablePlans[i] = optimizedPlan
		} else {
			tablePlans[i] = tablePlan
		}
	}

	// Phase 2: Optimize join order
	plan := p.optimizeJoinOrder(tablePlans, predicate)

	// Phase 3: Apply remaining predicates (both table-specific and join predicates)
	// TODO: apply only the join predicates
	if predicate != nil && len(tables) > 1 {
		// Apply all remaining predicates to the join result from Phase 2
		plan = NewSelectPlan(plan, predicate)
	}

	// Phase 4: Project the required fields
	plan = NewProjectPlan(plan, queryData.Fields())

	return plan, nil
}

// optimizeTableWithIndex attempts to use an index for selection on a single table
// and applies ALL table-specific predicates (both indexed and non-indexed)
func (p *BasicQueryPlanner) optimizeTableWithIndex(tablePlan Plan, tableName string, predicate *query.Predicate, tx *transaction.Transaction) (Plan, error) {
	tableSchema := tablePlan.Schema()

	tablePredicate := predicate.SelectSubPred(tableSchema)
	if tablePredicate == nil {
		return tablePlan, nil // No applicable predicate terms
	}

	// Get available indexes for this table
	indexInfoMap, err := p.metadataManager.GetIndexInfo(tableName, tx)
	if err != nil {
		return nil, err
	}

	// Find the best index to use
	bestPlan := tablePlan
	bestCost := tablePlan.BlocksAccessed()
	var indexedField string

	for fieldName, indexInfo := range indexInfoMap {
		// Check if predicate has equality condition on this field
		constant := tablePredicate.EquatesWithConstant(fieldName)
		if constant != nil {
			// Create index select plan
			var searchValue any
			if constant.IsString() {
				searchValue = constant.AsString()
			} else {
				searchValue = constant.AsInt()
			}

			indexPlan := NewIndexSelectPlan(tablePlan, indexInfo, searchValue)
			indexCost := indexPlan.BlocksAccessed()

			// Use index if it's more efficient
			if indexCost < bestCost {
				bestPlan = indexPlan
				bestCost = indexCost
				indexedField = fieldName
			}
		}
	}

	// Apply remaining table predicates (non-indexed conditions)
	if bestPlan != tablePlan {
		// Index was used - apply remaining non-indexed predicates
		remainingPredicate := p.removeIndexedTerm(tablePredicate, indexedField)
		if remainingPredicate != nil {
			bestPlan = NewSelectPlan(bestPlan, remainingPredicate)
		}
	} else {
		// No index used - apply all table predicates
		bestPlan = NewSelectPlan(bestPlan, tablePredicate)
	}

	return bestPlan, nil
}

// optimizeJoinOrder sorts tables by estimated cost and builds optimal join tree
func (p *BasicQueryPlanner) optimizeJoinOrder(tablePlans []Plan, predicate *query.Predicate) Plan {
	if len(tablePlans) == 1 {
		return tablePlans[0]
	}

	// Sort tables by estimated cost (most selective first)
	sort.Slice(tablePlans, func(i, j int) bool {
		return tablePlans[i].BlocksAccessed() < tablePlans[j].BlocksAccessed()
	})

	// Build join tree starting with most selective table
	result := tablePlans[0]
	for i := 1; i < len(tablePlans); i++ {
		// Try both join orders and pick the better one
		p1 := NewProductPlan(result, tablePlans[i])
		p2 := NewProductPlan(tablePlans[i], result)

		if p1.BlocksAccessed() < p2.BlocksAccessed() {
			result = p1
		} else {
			result = p2
		}
	}

	return result
}

// extractJoinPredicate extracts join conditions from the overall predicate
func (p *BasicQueryPlanner) extractJoinPredicate(predicate *query.Predicate, tablePlans []Plan) *query.Predicate {
	if len(tablePlans) <= 1 {
		return nil
	}

	// For now, return the full predicate for join conditions
	// In a more sophisticated implementation, we would extract only
	// the terms that involve fields from multiple tables
	combinedSchema := record.NewSchema()
	for _, plan := range tablePlans {
		combinedSchema.CopyAll(plan.Schema())
	}

	// Return predicate terms that apply to the combined schema
	// but weren't already handled by individual table optimizations
	return predicate.SelectSubPred(combinedSchema)
}

// extractRemainingPredicate determines what predicate terms still need to be applied
// after index optimization for single-table queries
func (p *BasicQueryPlanner) extractRemainingPredicate(predicate *query.Predicate, tablePlan Plan) *query.Predicate {
	// Get the table schema to extract relevant predicate terms
	tableSchema := tablePlan.Schema()
	tablePredicate := predicate.SelectSubPred(tableSchema)

	// For now, we use a simple heuristic:
	// - If this is an IndexSelectPlan, assume the index handles the predicate
	// - If this is a TablePlan, we need to apply the full predicate
	switch tablePlan.(type) {
	case *IndexSelectPlan:
		// Index already handles filtering, no additional predicate needed
		return nil
	default:
		// No index used, apply the full table predicate
		return tablePredicate
	}
}

// removeIndexedTerm creates a new predicate without the term that uses the indexed field
func (p *BasicQueryPlanner) removeIndexedTerm(predicate *query.Predicate, indexedField string) *query.Predicate {
	// Get all terms from the predicate
	terms := predicate.GetTerms()
	var result *query.Predicate

	for _, term := range terms {
		// Skip the term that equates the indexed field with a constant
		if term.GetLHS().IsFieldName() && term.GetLHS().AsFieldName() == indexedField && term.GetRHS().IsConstant() {
			continue // This term is handled by the index
		}
		// Add all other terms to the result
		if result == nil {
			result = query.NewPredicate(term)
		} else {
			termPred := query.NewPredicate(term)
			result.ConjunctWith(*termPred)
		}
	}

	return result
}
