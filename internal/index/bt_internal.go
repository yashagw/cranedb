package index

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// InternalNodeEntry represents an entry in an internal node
// It contains a key value and a block number pointing to a child node
type InternalNodeEntry struct {
	DataValue   any
	BlockNumber int
}

// NewInternalNodeEntry creates a new internal node entry
func NewInternalNodeEntry(dataValue any, blockNumber int) *InternalNodeEntry {
	return &InternalNodeEntry{
		DataValue:   dataValue,
		BlockNumber: blockNumber,
	}
}

// Internal represents a B-tree internal (directory) node
//
// IMPORTANT: Internal nodes are used ONLY for navigation - they do NOT contain actual data.
// Actual data is ALWAYS stored in leaf nodes.
//
// Internal nodes contain:
//   - Keys: Act as separators/routing values to navigate down the tree
//   - Block pointers: Point to child nodes (either other internal nodes or leaf nodes)
//
// Example internal node with entries [10→block5, 20→block8, 30→block12]:
//   - Entry at slot 0 (key 10→block5): block5 contains data for keys < 20
//     → This includes keys < 10, keys == 10, and keys >= 10 and < 20
//   - Entry at slot 1 (key 20→block8): block8 contains data for keys >= 20 and < 30
//   - Entry at slot 2 (key 30→block12): block12 contains data for keys >= 30
//
// Navigation rules (how findChildBlock routes keys):
//   - Keys < 20: FindSlotBefore returns slot 0 → use block5
//   - Keys == 20: FindSlotBefore returns slot 0, but slot+1 has exact match → increment to slot 1 → use block8
//   - Keys > 20 and < 30: FindSlotBefore returns slot 1 → use block8
//   - Keys >= 30: FindSlotBefore returns slot 2 (or higher) → use block12
//
// The keys (10, 20, 30) act as separators/routing values - they don't contain data themselves,
// they just tell us which child block to navigate to find the actual data.
//
// The flag field stores the level: 0 = leaf, 1+ = internal node level
type Internal struct {
	page     *BTPage
	filename string
}

// NewInternal creates a new internal node for the specified block
func NewInternal(tx *transaction.Transaction, blk *file.BlockID, layout *record.Layout) (*Internal, error) {
	page, err := NewBTPage(tx, blk, layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create BTPage: %w", err)
	}

	return &Internal{
		page:     page,
		filename: blk.Filename(),
	}, nil
}

// Close closes the internal node and unpins the underlying page
func (in *Internal) Close() error {
	if in.page != nil {
		return in.page.Close()
	}
	return nil
}

// findChildBlock finds which child block to follow for the given search key
// This is the core navigation method for traversing down the B-tree.
// Logic:
//   - Find the slot before the search key using FindSlotBefore
//   - If the next slot (slot+1) has an exact match with searchKey, use that slot instead
//   - Return the block number stored at that slot
func (in *Internal) findChildBlock(searchKey any) (*file.BlockID, error) {
	slot, err := in.page.FindSlotBefore(searchKey)
	if err != nil {
		return nil, fmt.Errorf("failed to find slot: %w", err)
	}

	numRecs, err := in.page.GetNumRecs()
	if err != nil {
		return nil, fmt.Errorf("failed to get record count: %w", err)
	}

	// Check if page is empty
	if numRecs == 0 {
		return nil, fmt.Errorf("cannot find child block in empty internal node")
	}

	// If slot is -1, it means searchKey is less than all entries, so use slot 0
	if slot < 0 {
		slot = 0
	}

	// If there's a next slot and it matches the search key exactly, use that slot
	if slot+1 < numRecs {
		nextVal, err := in.page.GetVal(slot+1, "dataval")
		if err != nil {
			return nil, fmt.Errorf("failed to get next value: %w", err)
		}
		if compareValues(nextVal, searchKey) == 0 {
			slot++
		}
	}

	// Get the block number at this slot
	blkNum, err := in.page.GetBlockNum(slot)
	if err != nil {
		return nil, fmt.Errorf("failed to get child number: %w", err)
	}

	return file.NewBlockID(in.filename, blkNum), nil
}

// Search traverses down the B-tree to find the leaf block containing the search key
// Returns the block number of the leaf node
//
// Process:
//  1. Find which child block to follow using findChildBlock
//  2. Move to that child block
//  3. Check if the child is an internal node (flag > 0) or leaf node (flag == 0)
//  4. If internal node, repeat from step 1
//  5. If leaf node, return the block number
//
// Example: Searching for key 25 in a 3-level tree:
//
//	Level 2 (root): [10→blk1, 30→blk2] → findChildBlock(25) → blk1
//	Level 1: Move to blk1, findChildBlock(25) → blk4
//	Level 0 (leaf): Move to blk4, flag==0 → Return blk4
func (in *Internal) Search(searchKey any) (int, error) {
	// Check the flag of the current page first
	flag, err := in.page.GetFlag()
	if err != nil {
		return 0, fmt.Errorf("failed to get flag: %w", err)
	}

	// If current page is a leaf-level directory (flag == 0), find child and return immediately
	if flag <= 0 {
		childBlk, err := in.findChildBlock(searchKey)
		if err != nil {
			return 0, fmt.Errorf("failed to find child block: %w", err)
		}
		return childBlk.Number(), nil
	}

	// Current page is an internal node (flag > 0), traverse down
	childBlk, err := in.findChildBlock(searchKey)
	if err != nil {
		return 0, fmt.Errorf("failed to find child block: %w", err)
	}

	// Keep traversing down while we're still in internal nodes
	for {
		// Save transaction and layout before closing current page
		tx := in.page.GetTransaction()
		layout := in.page.GetLayout()
		in.page.Close()

		// Move to the child block
		page, err := NewBTPage(tx, childBlk, layout)
		if err != nil {
			return 0, fmt.Errorf("failed to create child BTPage: %w", err)
		}
		in.page = page

		// Check if the child we moved to is a leaf node
		childFlag, err := in.page.GetFlag()
		if err != nil {
			return 0, fmt.Errorf("failed to get child flag: %w", err)
		}

		// If child is a leaf node, return its block number
		if childFlag <= 0 {
			return in.page.GetCurrentBlock().Number(), nil
		}

		// Child is also an internal node, find its child block
		childBlk, err = in.findChildBlock(searchKey)
		if err != nil {
			return 0, fmt.Errorf("failed to find child block: %w", err)
		}
	}
}

// insertEntry inserts a new entry into the current internal node page
// Returns an InternalNodeEntry if the page was split, nil otherwise
//
// Process:
//  1. Find the correct sorted position using FindSlotBefore
//  2. Insert the entry at that position
//  3. If page is full, split it at the middle and return the new entry for parent
//  4. If not full, return nil (no split needed)
//
// Example: Insert entry (25, block10) into page [10→blk1, 20→blk2, 30→blk3]
//
//	FindSlotBefore(25) returns slot=1 (after 20, before 30)
//	Insert at slot 2 → [10→blk1, 20→blk2, 25→blk10, 30→blk3]
//	If full, split at middle → return entry for parent
func (in *Internal) insertEntry(e *InternalNodeEntry) (*InternalNodeEntry, error) {
	// Find the insertion position (slot after the position returned by FindSlotBefore)
	newSlot, err := in.page.FindSlotBefore(e.DataValue)
	if err != nil {
		return nil, fmt.Errorf("failed to find slot: %w", err)
	}
	newSlot++ // Insert after the found position

	// Insert the entry
	if err := in.page.InsertInternalNode(newSlot, e.DataValue, e.BlockNumber); err != nil {
		return nil, fmt.Errorf("failed to insert entry: %w", err)
	}

	// Check if page is full
	isFull, err := in.page.IsFull()
	if err != nil {
		return nil, fmt.Errorf("failed to check if full: %w", err)
	}
	if !isFull {
		return nil, nil // No split needed
	}

	// Page is full, need to split
	flag, err := in.page.GetFlag()
	if err != nil {
		return nil, fmt.Errorf("failed to get flag: %w", err)
	}

	numRecs, err := in.page.GetNumRecs()
	if err != nil {
		return nil, fmt.Errorf("failed to get record count: %w", err)
	}

	// Split at the middle position
	splitPos := numRecs / 2
	splitVal, err := in.page.GetVal(splitPos, "dataval")
	if err != nil {
		return nil, fmt.Errorf("failed to get split value: %w", err)
	}

	// Split the page (keeping the same level/flag)
	newBlk, err := in.page.Split(splitPos, flag)
	if err != nil {
		return nil, fmt.Errorf("failed to split: %w", err)
	}

	// Return entry for parent to update
	return NewInternalNodeEntry(splitVal, newBlk.Number()), nil
}

// MakeNewRoot creates a new root when the current root splits
// This increases the tree height by one level
//
// Process:
//  1. Get the first value and level from current root
//  2. Split current root at position 0 (transfer ALL records to new block)
//  3. Create two entries: old root entry and new entry
//  4. Insert both entries into current (now empty) root
//  5. Increment the level flag
//
// Example: Root [10→blk1, 20→blk2, 30→blk3] splits, new entry is (25, blk4)
//
//	Split(0) → Current: [], New: [10→blk1, 20→blk2, 30→blk3]
//	Insert (10, newBlk) and (25, blk4) → Current: [10→newBlk, 25→blk4]
//	Set flag to level+1 → Now level 2 root
func (in *Internal) MakeNewRoot(e *InternalNodeEntry) error {
	// Get the first value from current root
	firstVal, err := in.page.GetVal(0, "dataval")
	if err != nil {
		return fmt.Errorf("failed to get first value: %w", err)
	}

	// Get current level
	level, err := in.page.GetFlag()
	if err != nil {
		return fmt.Errorf("failed to get flag: %w", err)
	}

	// Split at position 0 - this transfers ALL records to the new block
	// The current page becomes empty (ready for new root entries)
	newBlk, err := in.page.Split(0, level)
	if err != nil {
		return fmt.Errorf("failed to split: %w", err)
	}

	// Create entry for the old root (now in newBlk)
	oldRootEntry := NewInternalNodeEntry(firstVal, newBlk.Number())

	// Insert both entries into the current (now empty) root
	if _, err := in.insertEntry(oldRootEntry); err != nil {
		return fmt.Errorf("failed to insert old root entry: %w", err)
	}
	if _, err := in.insertEntry(e); err != nil {
		return fmt.Errorf("failed to insert new entry: %w", err)
	}

	// Increment the level (root is now one level higher)
	return in.page.SetFlag(level + 1)
}

// Insert inserts a new entry into the B-tree
// Returns an InternalNodeEntry if the current node was split, nil otherwise
//
// Process:
//  1. If this is a leaf node (flag == 0), delegate to leaf insertion
//     (Note: Internal nodes typically don't have flag == 0, but this handles edge cases)
//  2. Otherwise, find the appropriate child internal node
//  3. Recursively insert into that child
//  4. If child split, insert the returned entry into this node
//
// Example: Insert (25, blk10) into internal node at level 1
//
//	Find child block for 25 → blk3 (level 0 leaf)
//	Insert into leaf → returns (30, blk5) if leaf split
//	Insert (30, blk5) into this internal node
func (in *Internal) Insert(e *InternalNodeEntry) (*InternalNodeEntry, error) {
	flag, err := in.page.GetFlag()
	if err != nil {
		return nil, fmt.Errorf("failed to get flag: %w", err)
	}

	// If this is a leaf node (flag == 0), we shouldn't be in Internal
	// This case is typically handled by the caller (BTreeIndex)
	// But we handle it here for completeness
	if flag == 0 {
		return in.insertEntry(e)
	}

	// Find which child internal node to insert into
	childBlk, err := in.findChildBlock(e.DataValue)
	if err != nil {
		return nil, fmt.Errorf("failed to find child block: %w", err)
	}

	// Recursively insert into the child
	child, err := NewInternal(in.page.GetTransaction(), childBlk, in.page.GetLayout())
	if err != nil {
		return nil, fmt.Errorf("failed to create child internal: %w", err)
	}
	defer child.Close()

	myEntry, err := child.Insert(e)
	if err != nil {
		return nil, fmt.Errorf("failed to insert into child: %w", err)
	}

	// If child didn't split, we're done
	if myEntry == nil {
		return nil, nil
	}

	// Child split, so insert the new entry into this node
	return in.insertEntry(myEntry)
}
