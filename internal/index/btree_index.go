package index

import (
	"fmt"
	"math"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

var (
	_ Index = (*BTreeIndex)(nil)
)

// BTreeIndex implements a B-tree index structure
// It maintains a directory (internal nodes) and leaf nodes
// The directory is used for navigation, while leaves store actual data
type BTreeIndex struct {
	tx         *transaction.Transaction
	leafTbl    string
	dirTbl     string
	leafLayout *record.Layout
	dirLayout  *record.Layout
	rootBlk    *file.BlockID
	leaf       *Leaf
}

// NewBTreeIndex creates a new B-tree index
// If the index doesn't exist, it creates the initial structure:
//   - A leaf table with an initial empty leaf block (flag = -1)
//   - A directory table with a root block (flag = 0) containing a minimum value entry
func NewBTreeIndex(tx *transaction.Transaction, idxName string, leafLayout *record.Layout) (*BTreeIndex, error) {
	leafTbl := idxName + "leaf"
	dirTbl := idxName + "dir"

	// Deal with the leaves
	leafSize, err := tx.Size(leafTbl)
	if err != nil {
		return nil, fmt.Errorf("failed to get leaf table size: %w", err)
	}

	if leafSize == 0 {
		// Create initial leaf block
		blk, err := tx.Append(leafTbl)
		if err != nil {
			return nil, fmt.Errorf("failed to append leaf block: %w", err)
		}

		page, err := NewBTPage(tx, blk, leafLayout)
		if err != nil {
			return nil, fmt.Errorf("failed to create leaf BTPage: %w", err)
		}

		// Format with flag -1 (empty leaf)
		if err := page.Format(-1); err != nil {
			page.Close()
			return nil, fmt.Errorf("failed to format leaf page: %w", err)
		}
		page.Close()
	}

	// Deal with the directory
	// Create directory schema: block (child block number), id (unused), dataval (key)
	// The directory layout matches the internal node structure used in our implementation
	dirSchema := record.NewSchema()
	dirSchema.AddIntField("block") // Child block number
	dirSchema.AddIntField("id")    // Unused for internal nodes, but part of layout for compatibility
	// Copy dataval field from leaf layout (preserves type: int or string)
	datavalType := leafLayout.GetSchema().Type("dataval")
	datavalLength := leafLayout.GetSchema().Length("dataval")
	if datavalType == "int" {
		dirSchema.AddIntField("dataval")
	} else {
		dirSchema.AddStringField("dataval", datavalLength)
	}

	dirLayout := record.NewLayoutFromSchema(dirSchema)

	rootBlk := file.NewBlockID(dirTbl, 0)

	dirSize, err := tx.Size(dirTbl)
	if err != nil {
		return nil, fmt.Errorf("failed to get directory table size: %w", err)
	}

	if dirSize == 0 {
		// Create new root block
		_, err := tx.Append(dirTbl)
		if err != nil {
			return nil, fmt.Errorf("failed to append directory block: %w", err)
		}

		page, err := NewBTPage(tx, rootBlk, dirLayout)
		if err != nil {
			return nil, fmt.Errorf("failed to create root BTPage: %w", err)
		}

		// Format root with flag 0 (leaf level - root starts as leaf)
		if err := page.Format(0); err != nil {
			page.Close()
			return nil, fmt.Errorf("failed to format root page: %w", err)
		}

		// Insert initial directory entry with minimum value pointing to block 0
		// Get the type of dataval field to determine minimum value
		datavalType := leafLayout.GetSchema().Type("dataval")
		var minVal any
		if datavalType == "int" {
			minVal = math.MinInt
		} else {
			minVal = "" // Empty string for strings
		}

		if err := page.InsertInternalNode(0, minVal, 0); err != nil {
			page.Close()
			return nil, fmt.Errorf("failed to insert initial directory entry: %w", err)
		}

		page.Close()
	}

	return &BTreeIndex{
		tx:         tx,
		leafTbl:    leafTbl,
		dirTbl:     dirTbl,
		leafLayout: leafLayout,
		dirLayout:  dirLayout,
		rootBlk:    rootBlk,
	}, nil
}

// BeforeFirst positions the index before the first record matching the given search key
func (bti *BTreeIndex) BeforeFirst(searchKey any) error {
	// Close any existing leaf
	if err := bti.Close(); err != nil {
		return err
	}

	// Search for the leaf block containing the search key
	root, err := NewInternal(bti.tx, bti.rootBlk, bti.dirLayout)
	if err != nil {
		return fmt.Errorf("failed to create root internal: %w", err)
	}

	blkNum, err := root.Search(searchKey)
	if err != nil {
		root.Close()
		return fmt.Errorf("failed to search for leaf block: %w", err)
	}
	root.Close()

	// Create leaf node for the found block
	leafBlk := file.NewBlockID(bti.leafTbl, blkNum)
	leaf, err := NewLeaf(bti.tx, leafBlk, bti.leafLayout, searchKey)
	if err != nil {
		return fmt.Errorf("failed to create leaf: %w", err)
	}

	bti.leaf = leaf
	return nil
}

// Next moves to the next record with the same search key
// Returns false if there are no more matching records
func (bti *BTreeIndex) Next() (bool, error) {
	if bti.leaf == nil {
		return false, fmt.Errorf("leaf not initialized; call BeforeFirst first")
	}
	return bti.leaf.Next()
}

// GetDataRid returns the record identifier (RID) of the current record
func (bti *BTreeIndex) GetDataRid() (*record.RID, error) {
	if bti.leaf == nil {
		return nil, fmt.Errorf("leaf not initialized; call BeforeFirst first")
	}
	return bti.leaf.GetDataRid()
}

// Insert inserts a new record into the index with the given data value and record identifier
func (bti *BTreeIndex) Insert(dataVal any, dataRid *record.RID) error {
	// Position before the insertion point
	if err := bti.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("failed to position before first: %w", err)
	}

	// Insert into leaf
	e, err := bti.leaf.Insert(dataVal, dataRid)
	if err != nil {
		bti.leaf.Close()
		bti.leaf = nil
		return fmt.Errorf("failed to insert into leaf: %w", err)
	}
	bti.leaf.Close()
	bti.leaf = nil

	// If leaf didn't split, we're done
	if e == nil {
		return nil
	}

	// Leaf split, so insert the entry into the root
	root, err := NewInternal(bti.tx, bti.rootBlk, bti.dirLayout)
	if err != nil {
		return fmt.Errorf("failed to create root internal: %w", err)
	}
	defer root.Close()

	e2, err := root.Insert(e)
	if err != nil {
		return fmt.Errorf("failed to insert into root: %w", err)
	}

	// If root also split, make a new root
	if e2 != nil {
		if err := root.MakeNewRoot(e2); err != nil {
			return fmt.Errorf("failed to make new root: %w", err)
		}
	}

	return nil
}

// Delete deletes a record from the index with the given data value and record identifier
func (bti *BTreeIndex) Delete(dataVal any, dataRid *record.RID) error {
	// Position before the deletion point
	if err := bti.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("failed to position before first: %w", err)
	}

	// Delete from leaf
	if err := bti.leaf.Delete(dataRid); err != nil {
		bti.leaf.Close()
		bti.leaf = nil
		return fmt.Errorf("failed to delete from leaf: %w", err)
	}

	bti.leaf.Close()
	bti.leaf = nil
	return nil
}

// Close closes the index
func (bti *BTreeIndex) Close() error {
	if bti.leaf != nil {
		if err := bti.leaf.Close(); err != nil {
			return err
		}
		bti.leaf = nil
	}
	return nil
}

// SearchCost returns the cost of searching a B-tree index
// numBlocks: number of blocks in the index file
// rpb: records per block (fanout)
func SearchCost(numBlocks int, rpb int) int {
	if numBlocks <= 0 || rpb <= 1 {
		return 1
	}
	// Cost = 1 + log_rpb(numBlocks) = 1 + log(numBlocks) / log(rpb)
	// Add small epsilon before flooring to handle exact integer cases
	logValue := math.Log(float64(numBlocks)) / math.Log(float64(rpb))
	// Add tiny epsilon to ensure we round correctly for exact values
	return 1 + int(math.Floor(logValue+1e-10))
}
