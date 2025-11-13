package index

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/table"
	"github.com/yashagw/cranedb/internal/transaction"
	"github.com/yashagw/cranedb/internal/utils"
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

	searchKey any
	tableScan *table.TableScan
}

func NewHashIndex(transaction *transaction.Transaction, indexName string, layout *record.Layout) (*HashIndex, error) {
	return &HashIndex{
		indexName:   indexName,
		transaction: transaction,
		indexLayout: layout,
	}, nil
}

func (hi *HashIndex) BeforeFirst(searchKey any) error {
	hi.Close()
	hi.searchKey = searchKey

	hashValue, err := utils.HashValue(searchKey)
	if err != nil {
		return err
	}
	bucket := hashValue % NumBuckets
	indexTableName := fmt.Sprintf("%s-%d", hi.indexName, bucket)
	tableScan, err := table.NewTableScan(hi.transaction, hi.indexLayout, indexTableName)
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

		dataval, err := hi.tableScan.GetValue("dataval")
		if err != nil {
			return false, err
		}
		if dataval == hi.searchKey {
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

func (hi *HashIndex) Insert(dataVal any, dataRid *record.RID) error {
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

	return hi.tableScan.SetValue("dataval", dataVal)
}

func (hi *HashIndex) Delete(dataVal any, dataRid *record.RID) error {
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

// HashSearchCost returns the cost of searching an index file having
// the specified number of blocks.
// the method assumes that all buckets are about the same size,
// so the cost is simply the size of the bucket.
func HashSearchCost(numBlocks int) int {
	return numBlocks / NumBuckets
}
