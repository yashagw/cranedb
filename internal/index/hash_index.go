package index

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/scan"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ Index = (*HashIndex)(nil)
)

const (
	NumBuckets = 100
)

type HashIndex struct {
	transaction *transaction.Transaction
	indexName   string
	indexLayout *record.Layout

	searchKey *query.Constant
	tableScan *scan.TableScan
}

func NewHashIndex(transaction *transaction.Transaction, indexName string, layout *record.Layout) (*HashIndex, error) {
	return &HashIndex{
		indexName:   indexName,
		transaction: transaction,
		indexLayout: layout,
	}, nil
}

func (hi *HashIndex) BeforeFirst(searchKey *query.Constant) error {
	hi.Close()
	hi.searchKey = searchKey

	bucket := searchKey.Hash() % NumBuckets
	indexTableName := fmt.Sprintf("%s-%d", hi.indexName, bucket)
	tableScan, err := scan.NewTableScan(hi.transaction, hi.indexLayout, indexTableName)
	if err != nil {
		return err
	}
	hi.tableScan = tableScan
	return nil
}

func (hi *HashIndex) Close() error {
	if hi.tableScan != nil {
		hi.tableScan.Close()
		hi.tableScan = nil
	}
	return nil
}

func (hi *HashIndex) Next() (bool, error) {
	if hi.tableScan == nil {
		return false, fmt.Errorf("table scan not initialized; call BeforeFirst first")
	}
	if hi.searchKey == nil {
		return false, fmt.Errorf("search key not set; call BeforeFirst with a key")
	}

	for {
		hasNext, err := hi.tableScan.Next()
		if err != nil {
			return false, err
		}
		if !hasNext {
			return false, nil
		}

		matches, err := hi.currentRecordMatchesSearchKey()
		if err != nil {
			return false, err
		}
		if matches {
			return true, nil
		}
	}
}

func (hi *HashIndex) GetDataRid() (*record.RID, error) {
	if hi.tableScan == nil {
		return nil, fmt.Errorf("table scan not initialized; call BeforeFirst first")
	}

	blockNum, err := hi.tableScan.GetInt("block")
	if err != nil {
		return nil, err
	}
	slot, err := hi.tableScan.GetInt("id")
	if err != nil {
		return nil, err
	}

	return record.NewRID(blockNum, slot), nil
}

func (hi *HashIndex) Insert(dataVal *query.Constant, dataRid *record.RID) error {
	if err := hi.BeforeFirst(dataVal); err != nil {
		return err
	}
	if hi.tableScan == nil {
		return fmt.Errorf("table scan not initialized after BeforeFirst")
	}

	if err := hi.tableScan.Insert(); err != nil {
		return err
	}

	if err := hi.tableScan.SetInt("block", dataRid.Block()); err != nil {
		return err
	}
	if err := hi.tableScan.SetInt("id", dataRid.Slot()); err != nil {
		return err
	}

	return hi.setDataValue(dataVal)
}

func (hi *HashIndex) Delete(dataVal *query.Constant, dataRid *record.RID) error {
	if err := hi.BeforeFirst(dataVal); err != nil {
		return err
	}

	for {
		hasNext, err := hi.Next()
		if err != nil {
			return err
		}
		if !hasNext {
			return nil
		}

		currentRid, err := hi.GetDataRid()
		if err != nil {
			return err
		}
		if currentRid.Block() == dataRid.Block() && currentRid.Slot() == dataRid.Slot() {
			return hi.tableScan.Delete()
		}
	}
}

func (hi *HashIndex) currentRecordMatchesSearchKey() (bool, error) {
	if hi.searchKey == nil {
		return false, fmt.Errorf("search key not set; call BeforeFirst with a key")
	}

	if hi.searchKey.IsInt() {
		val, err := hi.tableScan.GetInt("dataval")
		if err != nil {
			return false, err
		}
		return val == hi.searchKey.AsInt(), nil
	}

	val, err := hi.tableScan.GetString("dataval")
	if err != nil {
		return false, err
	}
	return val == hi.searchKey.AsString(), nil
}

func (hi *HashIndex) setDataValue(dataVal *query.Constant) error {
	if dataVal == nil {
		return fmt.Errorf("data value cannot be nil")
	}

	if dataVal.IsInt() {
		return hi.tableScan.SetInt("dataval", dataVal.AsInt())
	}

	return hi.tableScan.SetString("dataval", dataVal.AsString())
}

// HashSearchCost returns the cost of searching an index file having
// the specified number of blocks.
// the method assumes that all buckets are about the same size,
// so the cost is simply the size of the bucket.
func HashSearchCost(numBlocks int) int {
	return numBlocks / NumBuckets
}
