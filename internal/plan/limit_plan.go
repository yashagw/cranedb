package plan

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
)

var (
	_ Plan = (*LimitPlan)(nil)
)

type LimitPlan struct {
	p      Plan
	limit  int
	offset int
}

func NewLimitPlan(p Plan, limit int, offset int) *LimitPlan {
	return &LimitPlan{
		p:      p,
		limit:  limit,
		offset: offset,
	}
}

func (p *LimitPlan) Open() (scan.Scan, error) {
	s, err := p.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewLimitScan(s, p.limit, p.offset), nil
}

func (p *LimitPlan) BlocksAccessed() int {
	return p.p.BlocksAccessed() // We still might need to access all blocks to get to the offset/limit
}

func (p *LimitPlan) RecordsOutput() int {
	records := p.p.RecordsOutput() - p.offset
	if records < 0 {
		records = 0
	}
	if p.limit > 0 && records > p.limit {
		records = p.limit
	}
	return records
}

func (p *LimitPlan) DistinctValues(fldname string) (int, error) {
	return p.p.DistinctValues(fldname)
}

func (p *LimitPlan) Schema() *record.Schema {
	return p.p.Schema()
}

func (p *LimitPlan) Explain(indent string, lastChild bool) string {
	return fmt.Sprintf("%sLimitPlan (Limit: %d, Offset: %d)\n%s", indent, p.limit, p.offset, p.p.Explain(indent+"  ", lastChild))
}
