package index

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// InternalNodeEntry represents an entry in an internal node
// This is what gets passed up the tree when a node splits
//
// Structure:
//   - DataValue: The key value that acts as a separator/routing value
//   - BlockNumber: The block number of the child node (leaf or internal)
//
// Example: InternalNodeEntry{DataValue: 30, BlockNumber: 5}
//
//	Meaning: "For keys >= 30, navigate to block 5"
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
// INTERNAL NODE STRUCTURE:
//
//	Each entry in an internal node contains:
//	- "dataval": The key value that acts as a separator/routing value
//	- "block": The block number of the child node (leaf or internal) to navigate to
//	- "id": UNUSED (always 0, kept for layout compatibility with leaf nodes)
//
//	Example internal entry: [dataval=25, block=5, id=0]
//	  Meaning: "For keys >= 25, navigate to block 5 (which may be a leaf or another internal node)"
//
// Flag semantics for internal pages (directory table):
//   - flag >= 1: Internal node level
//   - flag = 1: Points to leaf pages (leaf-level directory)
//   - flag = 2: Points to internal pages at level 1
//   - flag = 3: Points to internal pages at level 2
//   - etc.
//
// IMPORTANT: Internal pages are ALWAYS in the directory table and have flag >= 1.
// When traversing down the tree, we encounter leaf pages (from the leaf table)
// which have flag < 1 (flag = -1 for no overflow, or flag >= 0 for overflow block number).
// The check "flag < 1" is used to distinguish leaf pages from internal pages.
//
// Example internal node with entries [10→block5, 20→block8, 30→block12]:
//   - Entry at slot 0 (key 10→block5): block5 contains data for keys >= 10 and < 20
//   - Entry at slot 1 (key 20→block8): block8 contains data for keys >= 20 and < 30
//   - Entry at slot 2 (key 30→block12): block12 contains data for keys >= 30
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
//
// HOW IT WORKS:
//  1. Use FindSlotBefore to find the slot position before searchKey
//  2. If slot is -1 (searchKey < all entries), use slot 0 (first entry)
//  3. Check if the next slot (slot+1) has an exact match with searchKey
//     → If yes, use that slot (exact match takes precedence)
//  4. Return the block number stored at the selected slot
//
// Example: Internal node with entries [10→blk5, 20→blk8, 30→blk12]
//   - findChildBlock(15): FindSlotBefore(15)=0, slot+1=1 has key 20 (no match)
//     → Use slot 0 → return blk5
//   - findChildBlock(20): FindSlotBefore(20)=0, slot+1=1 has key 20 (exact match!)
//     → Use slot 1 → return blk8
//   - findChildBlock(25): FindSlotBefore(25)=1, slot+1=2 has key 30 (no match)
//     → Use slot 1 → return blk8
//   - findChildBlock(5): FindSlotBefore(5)=-1 (less than all)
//     → Use slot 0 → return blk5
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
	blkNum, err := in.page.GetChildBlockNum(slot)
	if err != nil {
		return nil, fmt.Errorf("failed to get child number: %w", err)
	}

	return file.NewBlockID(in.filename, blkNum), nil
}

// Search traverses down the B-tree to find the leaf block containing the search key
// Returns the block number of the leaf node (in the leaf table)
//
// FILE/BLOCK CONTEXT:
//   - This method operates on blocks in the DIRECTORY file
//   - When we read a block, we know it's from the directory file
//   - At level 1, child blocks are in the LEAF file
//   - At level 2+, child blocks are also in the DIRECTORY file
//
// HOW IT WORKS:
//  1. Check current page's flag:
//     - If flag == 1: This is a leaf-level directory (points directly to leaves)
//     → Find child block number, return immediately (child is in leaf file)
//     - If flag > 1: This points to other internal nodes
//     → Need to traverse deeper (children are also in directory file)
//  2. Enter loop to traverse down:
//     a. Move to child block (in directory file)
//     b. Check child's flag:
//     - If flag == 1: Level 1 internal → children are in LEAF file
//     → Find child block number, return it (caller creates leaf BlockID)
//     - If flag < 1: Unexpected (shouldn't happen in directory file)
//     → Treat as leaf, return block number
//     - If flag > 1: Higher level internal → children are in directory file
//     → Continue loop, find next child block
//
// FLAG INTERPRETATION:
//   - In directory file: flag >= 1 means internal page (level number)
//   - In directory file: flag < 1 is unexpected (shouldn't happen)
//   - When we reach level 1, we know children are in leaf file, so we return block number
//     and the caller creates a BlockID with the leaf file name
//
// DRY RUN: Searching for key 25 in a 3-level tree
//
//	Tree Structure:
//	  Directory Block 0 (root, flag=2): [10→1, 30→2]
//	  Directory Block 1 (level 1, flag=1): [15→4, 25→5]  ← children 4,5 are LEAF blocks
//	  Leaf Block 5: [25→RID1, 25→RID2, 30→RID3]
//
//	Step 1: Start at Directory Block 0
//	  → flag = 2 (not level 1)
//	  → findChildBlock(25) → BlockID(dirFile, 1)
//	  → Enter loop
//
//	Step 2: Move to Directory Block 1
//	  → flag = 1 (level 1!)
//	  → IsLevel1Internal(1) = true → Special case!
//	  → findChildBlock(25) → BlockID(dirFile, 5)
//	  → Return 5 (this is a leaf file block number)
//
//	Step 3: Caller creates Leaf BlockID(leafFile, 5)
//	  → Search complete!
//
// Note: The returned block number is in the LEAF table, not the directory table.
func (in *Internal) Search(searchKey any) (int, error) {
	// Check the flag of the current page first
	flag, err := in.page.GetFlag()
	if err != nil {
		return 0, fmt.Errorf("failed to get flag: %w", err)
	}

	// Internal pages start at level 1 (flag >= 1)
	// If current page is level 1 (points to leaf nodes), find child and return immediately
	if IsLevel1Internal(flag) {
		childBlk, err := in.findChildBlock(searchKey)
		if err != nil {
			return 0, fmt.Errorf("failed to find child block: %w", err)
		}
		return childBlk.Number(), nil
	}

	// Current page is an internal node (flag > 1), traverse down
	childBlk, err := in.findChildBlock(searchKey)
	if err != nil {
		return 0, fmt.Errorf("failed to find child block: %w", err)
	}

	// Keep traversing down while we're still in internal nodes
	for {
		tx := in.page.GetTransaction()
		layout := in.page.GetLayout()
		in.page.Close()

		// Move to the child block
		page, err := NewBTPage(tx, childBlk, layout)
		if err != nil {
			return 0, fmt.Errorf("failed to create child BTPage: %w", err)
		}
		in.page = page

		// Check the flag of the child we moved to
		childFlag, err := in.page.GetFlag()
		if err != nil {
			return 0, fmt.Errorf("failed to get child flag: %w", err)
		}

		// If child is level 1, it points to leaf nodes (in leaf file)
		// We need to handle this specially because findChildBlock returns
		// a BlockID with directory filename, but children are in leaf file
		if IsLevel1Internal(childFlag) {
			childBlk, err := in.findChildBlock(searchKey)
			if err != nil {
				return 0, fmt.Errorf("failed to find child block: %w", err)
			}
			// Return the block number (caller will create BlockID with leaf file name)
			return childBlk.Number(), nil
		}

		// If child is a leaf node (flag < 1), we're done traversing
		// This shouldn't normally happen in directory file, but we handle it
		if IsLeafPage(childFlag) {
			return in.page.GetCurrentBlock().Number(), nil
		}

		// Child is also an internal node (flag > 1), find its child block
		// and continue traversing
		childBlk, err = in.findChildBlock(searchKey)
		if err != nil {
			return 0, fmt.Errorf("failed to find child block: %w", err)
		}
	}
}

// insertEntry inserts a new entry into the current internal node page
// Returns an InternalNodeEntry if the page was split, nil otherwise
//
// HOW IT WORKS:
//  1. Find the correct sorted position using FindSlotBefore
//     → This maintains the sorted order of entries
//  2. Insert the entry at that position (shifts existing entries)
//  3. Check if page is full:
//     - If NOT full: Return nil (no split, done)
//     - If full: Split at middle position
//     → Split the page, keeping same level/flag
//     → Return InternalNodeEntry(splitKey, newBlockNumber) for parent
//
// Example: Insert entry (25, blk10) into page [10→blk1, 20→blk2, 30→blk3]
//
//	→ FindSlotBefore(25) = 1 (after 20, before 30)
//	→ Insert at slot 2 → [10→blk1, 20→blk2, 25→blk10, 30→blk3]
//	→ If not full, return nil
//	→ If full, split at middle (slot 2), return InternalNodeEntry(25, newBlk)
//
// Note: The split key is the first key in the new (right) page.
//
//	This key is used by the parent to route searches to the new block.
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
// IMPORTANT: This method does NOT change the block number of the root!
// The root always stays at the same block (directory block 0), but:
//   - Its contents change (old entries moved out, new entries inserted)
//   - Its flag changes (level increases: 1 → 2, 2 → 3, etc.)
//
// HOW IT WORKS:
//  1. Get the first key value from current root (needed for the old root entry)
//  2. Get the current level (flag value)
//  3. Split at position 0: This transfers ALL records to a new block
//     → Current root block becomes empty (ready for new entries)
//     → New block contains all the old root's entries
//  4. Create entry for the old root: (firstKey, newBlockNumber)
//  5. Insert both entries into the now-empty root block:
//     → Old root entry: points to the block containing old root's entries
//     → New entry: the entry that caused the split
//  6. Increment the level flag (root is now one level higher)
//
// Example: Root at level 1 [10→blk1, 20→blk2, 30→blk3] splits, new entry is (25, blk4)
//
//	Before:
//	  Directory Block 0: [10→blk1, 20→blk2, 30→blk3] (flag=1, FULL!)
//
//	After Split(0):
//	  Directory Block 0: [] (empty, flag still 1)
//	  Directory Block 1: [10→blk1, 20→blk2, 30→blk3] (old root moved here)
//
//	After Insert entries:
//	  Directory Block 0: [10→blk1, 25→blk4] (flag=2, now level 2!)
//	  Directory Block 1: [10→blk1, 20→blk2, 30→blk3] (unchanged)
//
// Result: Tree height increased by 1. Old root entries are now one level down.
// Root block number stays the same (block 0), but its contents and level changed.
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
// IMPORTANT: This method is called with a split entry from a CHILD node.
// The entry e represents a new block that was created when a child split.
//
// HOW IT WORKS:
//  1. Check the flag to determine what this node points to:
//     - If flag == 1: Points to LEAF nodes
//     → Insert the entry directly into this node (no recursion)
//     → The entry came from a leaf split, so we just add it here
//     - If flag > 1: Points to INTERNAL nodes
//     → Find which child internal node to insert into
//     → Recursively call Insert on that child
//     → If child split, insert the returned entry into this node
//  2. If this node becomes full and splits, return entry for parent
//
// Example: Insert InternalNodeEntry(30, blk5) into level 1 internal node
//
//	(This entry came from a leaf split)
//	→ flag == 1, so insert directly: [10→blk1, 20→blk2, 30→blk5]
//	→ If this node splits, return entry for parent (level 2)
//
// Example: Insert InternalNodeEntry(50, blk10) into level 2 internal node
//
//	→ flag == 2, so find child: findChildBlock(50) → blk3 (level 1 internal)
//	→ Recursively insert into blk3
//	→ If blk3 splits, insert the returned entry into this node
func (in *Internal) Insert(e *InternalNodeEntry) (*InternalNodeEntry, error) {
	flag, err := in.page.GetFlag()
	if err != nil {
		return nil, fmt.Errorf("failed to get flag: %w", err)
	}

	// Internal pages start at level 1 (flag >= 1)
	// If flag < 1, this is not an internal page (should not happen)
	if !IsInternalPage(flag) {
		return nil, fmt.Errorf("invalid internal page flag: %d (expected >= 1)", flag)
	}

	// If flag == 1, this internal node points to leaf nodes
	// The entry e is a split entry from a leaf, so insert it directly into this node
	if IsLevel1Internal(flag) {
		return in.insertEntry(e)
	}

	// Flag > 1: This internal node points to other internal nodes
	// Find which child internal node to insert into
	childBlk, err := in.findChildBlock(e.DataValue)
	if err != nil {
		return nil, fmt.Errorf("failed to find child block: %w", err)
	}

	// Recursively insert into the child internal node
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
