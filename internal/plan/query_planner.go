package plan

import (
	"sort"

	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/session"
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

// ExplainPlan creates a query plan and returns its string representation for EXPLAIN statements.
func (p *BasicQueryPlanner) ExplainPlan(explainData *parserdata.ExplainData, tx *transaction.Transaction, sess *session.Session) (string, error) {
	queryPlan, err := p.CreatePlan(explainData.QueryData(), tx, sess)
	if err != nil {
		return "", err
	}

	return queryPlan.Explain("", true), nil
}

func (p *BasicQueryPlanner) CreatePlan(queryData *parserdata.QueryData, tx *transaction.Transaction, sess *session.Session) (Plan, error) {
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
	plan := p.optimizeJoinOrder(tablePlans, predicate, tx, sess)

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
			if indexCost <= bestCost {
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

// optimizeJoinOrder sorts tables by estimated cost and builds optimal join tree.
// It considers materializing the inner relation in nested loop joins when beneficial.
func (p *BasicQueryPlanner) optimizeJoinOrder(tablePlans []Plan, predicate *query.Predicate, tx *transaction.Transaction, sess *session.Session) Plan {
	if len(tablePlans) == 1 {
		return tablePlans[0]
	}

	// Sort tables by estimated cost (most selective first)
	sort.Slice(tablePlans, func(i, j int) bool {
		return tablePlans[i].BlocksAccessed() < tablePlans[j].BlocksAccessed()
	})

	// Check if materialization is disabled via session variable
	noMaterialize := sess != nil && sess.GetBoolVariable("no_materialize")

	// Build join tree starting with most selective table
	result := tablePlans[0]
	for i := 1; i < len(tablePlans); i++ {
		p1 := NewProductPlan(result, tablePlans[i])
		p2 := NewProductPlan(tablePlans[i], result)

		// Check if materializing inner relation helps (only if not disabled)
		if !noMaterialize && p.shouldMaterializeForJoin(result, tablePlans[i], tx) {
			materialized := NewMaterializePlan(tx, tablePlans[i])
			p1 = NewProductPlan(result, materialized)
		}

		if !noMaterialize && p.shouldMaterializeForJoin(tablePlans[i], result, tx) {
			materialized := NewMaterializePlan(tx, result)
			p2 = NewProductPlan(tablePlans[i], materialized)
		}

		// Pick better option
		if p1.BlocksAccessed() < p2.BlocksAccessed() {
			result = p1
		} else {
			result = p2
		}
	}

	return result
}

// shouldMaterializeForJoin determines if materializing the inner plan in a join
// would reduce total cost.
func (p *BasicQueryPlanner) shouldMaterializeForJoin(outer Plan, inner Plan, tx *transaction.Transaction) bool {
	noMaterializeCost := outer.BlocksAccessed() + (outer.RecordsOutput() * inner.BlocksAccessed())

	materializePlan := NewMaterializePlan(tx, inner)
	withMaterializeCost := materializePlan.BlocksAccessed() + (outer.RecordsOutput() * materializePlan.BlocksAccessed())

	return withMaterializeCost < noMaterializeCost
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
