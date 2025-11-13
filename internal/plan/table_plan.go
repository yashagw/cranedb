package plan

import (
	"github.com/yashagw/cranedb/internal/metadata"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ Plan = (*TablePlan)(nil)
)

// TablePlan is the Plan for a base table.
type TablePlan struct {
	tableName string
	layout    *record.Layout
	tx        *transaction.Transaction
	statInfo  *metadata.StatInfo
}

func NewTablePlan(tableName string, tx *transaction.Transaction, md *metadata.Manager) (*TablePlan, error) {
	layout, err := md.GetTableLayout(tableName, tx)
	if err != nil {
		return nil, err
	}
	statInfo, err := md.GetStatInfo(tableName, layout, tx)
	if err != nil {
		return nil, err
	}
	return &TablePlan{
		tableName: tableName,
		layout:    layout,
		tx:        tx,
		statInfo:  statInfo,
	}, nil
}

func (p *TablePlan) Open() (scan.Scan, error) {
	scan, err := scan.NewTableScan(p.tx, p.layout, p.tableName)
	if err != nil {
		return nil, err
	}
	return scan, nil
}

// BlocksAccessed returns the number of blocks in the table.
func (p *TablePlan) BlocksAccessed() int {
	return p.statInfo.BlocksAccessed()
}

// RecordsOutput returns the number of records in the table.
func (p *TablePlan) RecordsOutput() int {
	return p.statInfo.RecordsOutput()
}

// DistinctValues returns the number of distinct values for the field in the table.
func (p *TablePlan) DistinctValues(fldname string) (int, error) {
	return p.statInfo.DistinctValues(fldname), nil
}

func (p *TablePlan) Schema() *record.Schema {
	return p.layout.GetSchema()
}
