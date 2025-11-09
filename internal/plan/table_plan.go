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

func NewTablePlan(tableName string, tx *transaction.Transaction, md *metadata.Manager) *TablePlan {
	layout, err := md.GetTableLayout(tableName, tx)
	if err != nil {
		panic(err)
	}
	statInfo := md.GetStatInfo(tableName, layout, tx)
	return &TablePlan{
		tableName: tableName,
		layout:    layout,
		tx:        tx,
		statInfo:  statInfo,
	}
}

func (p *TablePlan) Open() scan.Scan {
	return record.NewTableScan(p.tx, p.layout, p.tableName)
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
func (p *TablePlan) DistinctValues(fldname string) int {
	return p.statInfo.DistinctValues(fldname, p.tx, p.tableName)
}

func (p *TablePlan) Schema() *record.Schema {
	return p.layout.GetSchema()
}
