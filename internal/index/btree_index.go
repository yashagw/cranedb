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
//
// ARCHITECTURE:
//
//	The B-tree consists of two separate tables:
//	1. Leaf table (idxName + "leaf"): Contains actual data entries
//	   - Each entry: [dataval=key, block=dataBlock, id=dataSlot]
//	   - Points to actual records in the indexed table
//	2. Directory table (idxName + "dir"): Contains navigation entries
//	   - Each entry: [dataval=key, block=childBlock, id=0]
//	   - Points to child nodes (leaf or internal)
//
// INITIAL STATE:
//   - When first created: Only leaf table exists with one empty leaf block (block 0)
//   - Directory table is empty (root is still a leaf)
//   - When first leaf splits: Directory table is created with root pointing to both leaf blocks
//
// TREE GROWTH:
//   - Small tree: Root is a leaf (no directory needed)
//   - After first split: Directory created, root is level 1 internal node pointing to leaves
//   - After root splits: New root level created (level 2), tree height increases
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
//   - Directory table starts empty - created when first leaf split occurs
func NewBTreeIndex(tx *transaction.Transaction, idxName string, leafLayout *record.Layout) (*BTreeIndex, error) {
	leafTbl := idxName + "leaf"
	dirTbl := idxName + "dir"

	// Deal with the leaves
	leafSize, err := tx.Size(leafTbl)
	if err != nil {
		return nil, fmt.Errorf("failed to get leaf table size: %w", err)
	}

	if leafSize == 0 {
		// Create initial leaf block (this is the root when tree is small)
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
//
// HOW IT WORKS:
//  1. Check if directory table exists:
//     - If empty: Root is still a leaf → use leaf block 0 directly
//     - If exists: Root is an internal node → use Internal.Search to find leaf block
//  2. Create a Leaf cursor positioned before the first matching record
//  3. Store the leaf cursor for use by Next() and GetDataRid()
//
// Example: BeforeFirst(25) when directory is empty
//
//	→ Use leaf block 0, create Leaf cursor at position before first record >= 25
//
// Example: BeforeFirst(25) when directory exists
//
//	→ Internal.Search(25) → traverses tree → returns leaf block number (e.g., 3)
//	→ Create Leaf cursor for block 3, positioned before first record >= 25
func (bti *BTreeIndex) BeforeFirst(searchKey any) error {
	// Close any existing leaf
	if err := bti.Close(); err != nil {
		return err
	}

	// Check if directory table exists (if empty, root is still a leaf)
	dirSize, err := bti.tx.Size(bti.dirTbl)
	if err != nil {
		return fmt.Errorf("failed to get directory size: %w", err)
	}

	var blkNum int
	if dirSize == 0 {
		// Directory is empty - root is still a leaf (block 0)
		blkNum = 0
	} else {
		// Directory exists - search through internal nodes
		root, err := NewInternal(bti.tx, bti.rootBlk, bti.dirLayout)
		if err != nil {
			return fmt.Errorf("failed to create root internal: %w", err)
		}

		blkNum, err = root.Search(searchKey)
		if err != nil {
			root.Close()
			return fmt.Errorf("failed to search for leaf block: %w", err)
		}
		root.Close()
	}

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
//
// HOW IT WORKS:
//
// The Insert method handles three scenarios:
//  1. Simple insert (no split): Leaf has room, insert and done
//  2. First split (creates directory): First time a leaf splits, create directory table
//  3. Subsequent splits (updates directory): Update directory, may grow tree height
//
// PHASE 1: Position and Insert into Leaf
//  1. Position cursor at correct leaf using BeforeFirst
//  2. Insert record into leaf using Leaf.Insert
//  3. Check return value:
//     - e == nil: No split occurred → Done!
//     - e != nil: Leaf split occurred → Need to update directory
//
// PHASE 2A: First Split - Create Directory (if dirSize == 0)
//
//	This happens when the directory table doesn't exist yet (first leaf split).
//
//	Example: Leaf block 0 splits: [10, 20] | [30, 40] (new block 1)
//	  → Leaf.Insert returns: InternalNodeEntry(30, 1)
//	  → Get first value from block 0: firstVal = 10
//	  → Create directory table (first time)
//	  → Create root internal node (level 1, flag = 1)
//	  → Insert two entries:
//	     - [10 → block 0] (original leaf)
//	     - [30 → block 1] (new split block from e)
//	  → Result: Root = [10→blk0, 30→blk1]
//
// PHASE 2B: Directory Exists - Insert into Root (if dirSize > 0)
//
//	This happens when directory already exists.
//
//	Example 1: Root doesn't split
//	  → Leaf splits → e = (50, blk5)
//	  → Insert into root: [10→0, 30→1, 50→5]
//	  → Root not full → e2 = nil → Done
//
//	Example 2: Root splits (tree grows)
//	  → Leaf splits → e = (50, blk5)
//	  → Root is full: [10→0, 20→1, 30→2, 40→3]
//	  → Insert (50, blk5) → root splits
//	  → MakeNewRoot creates new level 2 root:
//	     Old root (level 1): [10→0, 20→1, 30→2, 40→3] → moved to new block
//	     New root (level 2): [10→oldRootBlock, 50→newBlock]
//	  → Tree height increases from 1 to 2
//
// TREE EVOLUTION EXAMPLE:
//
//	Stage 1: First Insert
//	  Insert(10, RID{100,1})
//	    → Leaf block 0: [10]
//	    → No split (e = nil) → Done
//
//	Stage 2: Fill Leaf, Then Split
//	  Insert(20), Insert(30), Insert(40)
//	    → Leaf block 0: [10, 20, 30, 40]
//	  Insert(50)  ← Makes leaf full
//	    → Leaf splits: [10, 20] | [30, 40, 50] (new block 1)
//	    → e = InternalNodeEntry(30, 1)
//	    → Directory empty → Create directory!
//	    → Root: [10→0, 30→1]
//
//	Stage 3: More Inserts, Root Splits
//	  ... many more inserts ...
//	  Insert(100)  ← Causes root to split
//	    → Leaf splits → e = (100, blk10)
//	    → Insert into root → root is full → root splits
//	    → MakeNewRoot creates level 2 root
//	    → Tree height: 1 → 2
func (bti *BTreeIndex) Insert(dataVal any, dataRid *record.RID) error {
	// ============================================================
	// PHASE 1: Position and Insert into Leaf
	// ============================================================
	// Position cursor at the correct leaf block for this key
	if err := bti.BeforeFirst(dataVal); err != nil {
		return fmt.Errorf("failed to position before first: %w", err)
	}

	// Insert the record into the leaf
	// Returns InternalNodeEntry if leaf split, nil otherwise
	e, err := bti.leaf.Insert(dataVal, dataRid)
	if err != nil {
		bti.leaf.Close()
		bti.leaf = nil
		return fmt.Errorf("failed to insert into leaf: %w", err)
	}
	bti.leaf.Close()
	bti.leaf = nil

	// If leaf didn't split, we're done - no directory update needed
	if e == nil {
		return nil
	}

	// ============================================================
	// PHASE 2: Leaf Split - Update Directory
	// ============================================================
	// Leaf split occurred, so we need to create or update the directory
	dirSize, err := bti.tx.Size(bti.dirTbl)
	if err != nil {
		return fmt.Errorf("failed to get directory size: %w", err)
	}

	if dirSize == 0 {
		// ============================================================
		// PHASE 2A: First Split - Create Directory
		// ============================================================
		// This is the first time a leaf has split, so we need to create
		// the directory table and root internal node.
		//
		// We need two entries in the root:
		//   1. Entry for the original leaf block 0 (need its first key)
		//   2. Entry from the split (e.DataValue → e.BlockNumber)

		// Get the first value from the original leaf (block 0) to create the entry
		leafBlk0 := file.NewBlockID(bti.leafTbl, 0)
		leafPage, err := NewBTPage(bti.tx, leafBlk0, bti.leafLayout)
		if err != nil {
			return fmt.Errorf("failed to read leaf block 0: %w", err)
		}

		firstVal, err := leafPage.GetVal(0, "dataval")
		leafPage.Close()
		if err != nil {
			return fmt.Errorf("failed to get first value from leaf: %w", err)
		}

		// Create directory table (first time) - this creates block 0
		_, err = bti.tx.Append(bti.dirTbl)
		if err != nil {
			return fmt.Errorf("failed to append directory block: %w", err)
		}

		// Create root internal node at directory block 0
		rootPage, err := NewBTPage(bti.tx, bti.rootBlk, bti.dirLayout)
		if err != nil {
			return fmt.Errorf("failed to create root BTPage: %w", err)
		}

		// Format root with flag 1 (level 1 - points directly to leaf nodes)
		if err := rootPage.Format(1); err != nil {
			rootPage.Close()
			return fmt.Errorf("failed to format root page: %w", err)
		}

		// Insert two entries into root:
		//   1. [firstVal → block 0] - original leaf block
		//   2. [e.DataValue → e.BlockNumber] - new split block
		// Example: Root = [10→0, 30→1] means:
		//   - Keys < 30 go to block 0
		//   - Keys >= 30 go to block 1
		if err := rootPage.InsertInternalNode(0, firstVal, 0); err != nil {
			rootPage.Close()
			return fmt.Errorf("failed to insert entry for block 0: %w", err)
		}
		if err := rootPage.InsertInternalNode(1, e.DataValue, e.BlockNumber); err != nil {
			rootPage.Close()
			return fmt.Errorf("failed to insert entry for new block: %w", err)
		}

		rootPage.Close()
		return nil
	}

	// ============================================================
	// PHASE 2B: Directory Exists - Insert into Root
	// ============================================================
	// Directory already exists, so insert the split entry into the root.
	// If the root is full and splits, MakeNewRoot will increase tree height.

	// Open root internal node
	root, err := NewInternal(bti.tx, bti.rootBlk, bti.dirLayout)
	if err != nil {
		return fmt.Errorf("failed to create root internal: %w", err)
	}
	defer root.Close()

	// Insert the split entry into root
	// Returns InternalNodeEntry if root split, nil otherwise
	e2, err := root.Insert(e)
	if err != nil {
		return fmt.Errorf("failed to insert into root: %w", err)
	}

	// If root also split, make a new root (increases tree height by 1)
	// This happens when the root internal node becomes full and splits.
	// MakeNewRoot creates a new level above the current root.
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
	return 1 + int(math.Log(float64(numBlocks))/math.Log(float64(rpb)))
}
