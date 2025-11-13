package metadata

import (
	"github.com/yashagw/cranedb/internal/index"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// IndexInfo contains info necessary to estimate index costs and open the index
type IndexInfo struct {
	indexName   string
	fieldName   string
	tableSchema *record.Schema
	transaction *transaction.Transaction
	indexLayout *record.Layout
	statInfo    *StatInfo
}

// NewIndexInfo creates an IndexInfo object for the specified index.
func NewIndexInfo(indexName string, fieldName string, tableSchema *record.Schema,
	transaction *transaction.Transaction, statInfo *StatInfo) *IndexInfo {
	ii := &IndexInfo{
		indexName:   indexName,
		fieldName:   fieldName,
		transaction: transaction,
		tableSchema: tableSchema,
		statInfo:    statInfo,
	}
	ii.indexLayout = ii.CreateIndexLayout()
	return ii
}

func (ii *IndexInfo) Open() (*index.HashIndex, error) {
	index, err := index.NewHashIndex(ii.transaction, ii.indexName, ii.indexLayout)
	if err != nil {
		return nil, err
	}
	return index, nil
}

// BlocksAccessed gives estimates no of blocks to search for a single key
func (ii *IndexInfo) BlocksAccessed() int {
	recordsPerBlock := ii.transaction.BlockSize() / ii.indexLayout.GetSlotSize()
	numBlocks := ii.statInfo.RecordsOutput() / recordsPerBlock
	return index.HashSearchCost(numBlocks)
}

// RecordsOutput gives estimates no of records for index key
func (ii *IndexInfo) RecordsOutput() int {
	return ii.statInfo.RecordsOutput() / ii.statInfo.DistinctValues(ii.fieldName)
}

// DistinctValues gives distinct values for the field
// if the field is the index field, return 1
func (ii *IndexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}
	return ii.statInfo.DistinctValues(fieldName)
}

// CreateIndexLayout builds the layout for index records: block, id, dataval
func (ii *IndexInfo) CreateIndexLayout() *record.Layout {
	sch := record.NewSchema()
	sch.AddIntField("block")
	sch.AddIntField("id")

	if ii.tableSchema.Type(ii.fieldName) == "int" {
		sch.AddIntField("dataval")
	} else {
		fldLen := ii.tableSchema.Length(ii.fieldName)
		sch.AddStringField("dataval", fldLen)
	}

	return record.NewLayoutFromSchema(sch)
}
