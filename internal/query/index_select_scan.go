package query

import (
	"github.com/yashagw/cranedb/internal/index"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/table"
)

var (
	_ scan.Scan = (*IndexSelectScan)(nil)
)

type IndexSelectScan struct {
	tableScan *table.TableScan
	index     index.Index
	value     any
}

func NewIndexSelectScan(tableScan *table.TableScan, idx index.Index, value any) (*IndexSelectScan, error) {
	iss := &IndexSelectScan{
		tableScan: tableScan,
		index:     idx,
		value:     value,
	}
	err := iss.BeforeFirst()
	if err != nil {
		return nil, err
	}
	return iss, nil
}

func (iss *IndexSelectScan) BeforeFirst() error {
	return iss.index.BeforeFirst(iss.value)
}

func (iss *IndexSelectScan) Next() (bool, error) {
	next, err := iss.index.Next()
	if !next || err != nil {
		return next, err
	}
	dataRID, err := iss.index.GetDataRid()
	if err != nil {
		return false, err
	}
	return true, iss.tableScan.MoveToRID(dataRID)
}

func (iss *IndexSelectScan) GetInt(fldname string) (int, error) {
	return iss.tableScan.GetInt(fldname)
}

func (iss *IndexSelectScan) GetString(fldname string) (string, error) {
	return iss.tableScan.GetString(fldname)
}

func (iss *IndexSelectScan) GetValue(fldname string) (any, error) {
	return iss.tableScan.GetValue(fldname)
}

func (iss *IndexSelectScan) HasField(fieldName string) bool {
	return iss.tableScan.HasField(fieldName)
}

func (iss *IndexSelectScan) Close() {
	iss.index.Close()
	iss.tableScan.Close()
}
