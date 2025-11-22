package index

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// Leaf represents a B-tree leaf node that stores key-value pairs
// Each entry contains a key (dataval) and a record identifier (RID)
//
// Flag semantics for leaf pages:
//   - flag = -1: Regular leaf page with no overflow
//   - flag >= 0: Leaf page with overflow (flag contains the block number of the overflow block)
//
// Overflow Handling:
//   - When a leaf page is full and all records have the same key, we create an "overflow" block
//   - The flag field stores the block number of the overflow block
//   - This allows us to handle many duplicate keys efficiently
//   - Overflow blocks are also leaf pages (flag = -1 or flag >= 0 for chained overflow)
type Leaf struct {
	searchKey   any
	page        *BTPage // The B-tree page containing the leaf node data
	currentSlot int
	filename    string
}

// NewLeaf creates a new B-tree leaf node for the specified block
// It positions the cursor immediately before the first record that matches the search key
//
// HOW IT WORKS:
//  1. Opens the leaf page at the specified block
//  2. Uses FindSlotBefore to locate the position before the first record >= searchKey
//  3. Sets currentSlot to that position (so Next() will find the first matching record)
//
// Example: NewLeaf(tx, blk, layout, 25) on a leaf page with keys [10, 20, 30, 40]:
//
//	FindSlotBefore(25) returns 1 (position after 20, before 30)
//	The leaf node is created and currentSlot is set to 1
//	Next() will increment to slot 2 and find key 30 (first record >= 25)
//
// Note: If searchKey matches existing records, currentSlot points before them,
// so Next() will find all matching records in order.
func NewLeaf(tx *transaction.Transaction, blk *file.BlockID, layout *record.Layout, searchKey any) (*Leaf, error) {
	page, err := NewBTPage(tx, blk, layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create BTPage: %w", err)
	}

	// Find the position before the first matching record
	currentSlot, err := page.FindSlotBefore(searchKey)
	if err != nil {
		page.Close()
		return nil, fmt.Errorf("failed to find slot: %w", err)
	}

	return &Leaf{
		searchKey:   searchKey,
		page:        page,
		currentSlot: currentSlot,
		filename:    blk.Filename(),
	}, nil
}

// Close closes the leaf node and unpins the underlying page
func (ln *Leaf) Close() error {
	if ln.page != nil {
		return ln.page.Close()
	}
	return nil
}

// Next moves to the next record with the same search key
// Returns true if a matching record is found, false otherwise
//
// HOW IT WORKS:
//  1. Increments currentSlot to check the next record
//  2. If we've reached the end of the page, tries to move to overflow block (if exists)
//  3. Checks if the current record's key matches searchKey
//  4. Returns true if match found, false if no more matches
//
// Overflow Handling:
//   - If we reach the end of the current page and flag >= 0, there's an overflow block
//   - We only follow overflow if searchKey matches the first key in the current page
//   - This ensures we only follow overflow for the correct key
//   - Overflow blocks can chain (overflow block can have its own overflow)
//
// Example: Searching for key 25 in a page with [25, 25, 30, 40] and overflow block 5:
//   - currentSlot starts at 1 (before first 25)
//   - Next() → currentSlot=2, finds key 25, returns true
//   - Next() → currentSlot=3, finds key 30 (different), returns false
//   - If overflow exists and first key is 25, we'd move to overflow and continue
func (ln *Leaf) Next() (bool, error) {
	for {
		ln.currentSlot++

		numRecs, err := ln.page.GetNumRecs()
		if err != nil {
			return false, fmt.Errorf("failed to get record count: %w", err)
		}

		// If we've reached the end of the current page, try overflow block
		if ln.currentSlot >= numRecs {
			moved, err := ln.tryOverflow()
			if err != nil {
				return false, err
			}
			if !moved {
				// No overflow block, we're done
				return false, nil
			}
			// Successfully moved to overflow block, continue loop to check first record
			continue
		}

		// Check if current record matches search key
		dataVal, err := ln.page.GetVal(ln.currentSlot, "dataval")
		if err != nil {
			return false, fmt.Errorf("failed to get data value: %w", err)
		}

		// If it matches, we found it
		if compareValues(dataVal, ln.searchKey) == 0 {
			return true, nil
		}

		// If it doesn't match, we've passed all records with the matching key
		// Only try overflow if we're at the end of the page (handled above)
		// If we encounter a different key, there are no more matching records
		return false, nil
	}
}

// tryOverflow attempts to move to an overflow block if one exists
// Overflow blocks are used when a leaf page is full of duplicate keys
//
// Logic:
//   - If flag >= 0, it contains the block number of the overflow block
//   - Regular leaf pages have flag = -1 (no overflow)
//   - We only follow overflow if searchKey matches the first key in the current page
//   - This ensures we only follow overflow for the correct key
func (ln *Leaf) tryOverflow() (bool, error) {
	flag, err := ln.page.GetFlag()
	if err != nil {
		return false, fmt.Errorf("failed to get flag: %w", err)
	}

	// Check if there's an overflow block
	// Regular leaf pages have flag = -1 (no overflow)
	// Leaf pages with overflow have flag >= 0 (flag contains overflow block number)
	if !HasOverflow(flag) {
		return false, nil // No overflow block
	}

	// Check if page is empty - if so, don't try overflow
	numRecs, err := ln.page.GetNumRecs()
	if err != nil {
		return false, fmt.Errorf("failed to get record count: %w", err)
	}
	if numRecs == 0 {
		return false, nil // Empty page, no overflow to follow
	}

	// Check if searchKey matches the first key in the current page
	firstKey, err := ln.page.GetVal(0, "dataval")
	if err != nil {
		return false, fmt.Errorf("failed to get first key: %w", err)
	}

	if compareValues(firstKey, ln.searchKey) != 0 {
		return false, nil
	}

	// Move to overflow block
	tx := ln.page.GetTransaction()
	layout := ln.page.GetLayout()
	ln.page.Close()

	// Get overflow block number from flag
	overflowBlockNum, err := GetOverflowBlock(flag)
	if err != nil {
		return false, fmt.Errorf("failed to get overflow block number: %w", err)
	}
	overflowBlk := file.NewBlockID(ln.filename, overflowBlockNum)

	// Check if the overflow block actually exists in the file
	fileSize, err := tx.Size(ln.filename)
	if err != nil {
		return false, fmt.Errorf("failed to get file size: %w", err)
	}
	if overflowBlk.Number() >= fileSize {
		return false, fmt.Errorf("overflow block %s does not exist in file %s", overflowBlk.String(), ln.filename)
	}

	page, err := NewBTPage(tx, overflowBlk, layout)
	if err != nil {
		return false, fmt.Errorf("failed to create overflow page: %w", err)
	}
	ln.page = page
	ln.currentSlot = -1 // Set to -1 so next Next() will check slot 0

	return true, nil
}

// GetDataRid returns the RID of the current record
func (ln *Leaf) GetDataRid() (*record.RID, error) {
	return ln.page.GetDataRid(ln.currentSlot)
}

// Insert inserts a new record into the leaf node
// Returns an InternalNodeEntry if the page was split, nil otherwise
//
// HOW IT WORKS:
//  1. Special case: If page has overflow and inserting before first record
//     → Split at position 0 to maintain order, clear overflow flag
//  2. Normal insertion: Find correct sorted position using FindSlotBefore
//     → Insert the new entry at that position
//  3. Check if page is full:
//     a. If NOT full: Return nil (no split, done)
//     b. If full AND all keys are the same: Create overflow block
//     → Split at position 1, keep first record, move rest to overflow
//     → Set flag to overflow block number, return nil (no parent update)
//     c. If full AND keys are different: Split in middle
//     → Split page, return InternalNodeEntry for parent to update
//
// SPLIT BEHAVIOR:
//   - Overflow split: All records have same key → create overflow, no parent update
//   - Regular split: Different keys → split in middle, return entry for parent
//   - Split entry contains: (splitKey, newBlockNumber)
//     → Parent uses this to route future searches to the new block
//
// Example: Insert(25, RID{100,5}) into page [10, 20, 30, 40] (not full)
//
//	→ FindSlotBefore(25) = 1, insert at slot 2
//	→ Result: [10, 20, 25, 30, 40], return nil (no split)
//
// Example: Insert(35, RID{200,10}) makes page full [10, 20, 30, 35, 40]
//
//	→ Split at position 2 (middle), splitKey = 30
//	→ Current: [10, 20], New: [30, 35, 40]
//	→ Return InternalNodeEntry(30, newBlockNumber) for parent
func (ln *Leaf) Insert(dataVal any, dataRid *record.RID) (*InternalNodeEntry, error) {
	flag, err := ln.page.GetFlag()
	if err != nil {
		return nil, fmt.Errorf("failed to get flag: %w", err)
	}

	numRecs, err := ln.page.GetNumRecs()
	if err != nil {
		return nil, fmt.Errorf("failed to get record count: %w", err)
	}

	// Special case: If page has overflow and inserting before first record
	// We need to split at position 0 to maintain order
	//
	// Example: Current page has overflow (flag=5, pointing to block 5):
	// [30, 35, 40, _, _] with overflow block 5: [30, 30, 30, 30]
	// Insert(25): 25 < 30 (first record), so split at position 0
	// Result: Current page: [25] (flag=-1), New block: [30, 35, 40] (inherits overflow)
	// Regular leaf pages have flag = -1 (no overflow), flag >= 0 means overflow exists
	if HasOverflow(flag) && numRecs > 0 {
		firstVal, err := ln.page.GetVal(0, "dataval")
		if err != nil {
			return nil, fmt.Errorf("failed to get first value: %w", err)
		}

		// If inserting before first record, split at position 0
		if compareValues(dataVal, firstVal) < 0 {
			firstVal, err := ln.page.GetVal(0, "dataval")
			if err != nil {
				return nil, fmt.Errorf("failed to get first value: %w", err)
			}

			newBlk, err := ln.page.Split(0, flag)
			if err != nil {
				return nil, fmt.Errorf("failed to split: %w", err)
			}

			ln.currentSlot = 0
			ln.page.SetFlag(-1) // Clear overflow flag

			// Insert the new record
			if err := ln.page.InsertLeaf(ln.currentSlot, dataVal, dataRid); err != nil {
				return nil, fmt.Errorf("failed to insert: %w", err)
			}

			// Return entry for parent to update
			return NewInternalNodeEntry(firstVal, newBlk.Number()), nil
		}
	}

	// Normal insertion: find correct position and insert
	//
	// Example: Page [10, 30, _, _, _], Insert(25)
	// FindSlotBefore(25) returns 0 (position before 30)
	// slot = 0 + 1 = 1, InsertLeaf(1, 25) → [10, 25, 30, _, _]
	slot, err := ln.page.FindSlotBefore(dataVal)
	if err != nil {
		return nil, fmt.Errorf("failed to find insertion slot: %w", err)
	}
	slot++ // Insert after the found position

	if err := ln.page.InsertLeaf(slot, dataVal, dataRid); err != nil {
		return nil, fmt.Errorf("failed to insert: %w", err)
	}

	// Check if page is full
	isFull, err := ln.page.IsFull()
	if err != nil {
		return nil, fmt.Errorf("failed to check if full: %w", err)
	}
	if !isFull {
		return nil, nil // No split needed
	}

	// Page is full, need to split
	// Get first and last keys to determine split strategy
	//
	// Example: After inserting, page is now full with 5 records (capacity reached)
	// Two possible scenarios:
	// 1. All same keys [25, 25, 25, 25, 25] → Create overflow block
	// 2. Different keys [10, 20, 30, 35, 40] → Split in middle
	numRecs, err = ln.page.GetNumRecs()
	if err != nil {
		return nil, fmt.Errorf("failed to get record count: %w", err)
	}

	firstKey, err := ln.page.GetVal(0, "dataval")
	if err != nil {
		return nil, fmt.Errorf("failed to get first key: %w", err)
	}

	lastKey, err := ln.page.GetVal(numRecs-1, "dataval")
	if err != nil {
		return nil, fmt.Errorf("failed to get last key: %w", err)
	}

	// If all keys are the same, create overflow block
	//
	// Example: Page full with [25, 25, 25, 25, 25] after Insert(25)
	// All keys same (25), so create overflow: Split(1, flag)
	// Result: Current page: [25] (flag=newBlockNumber), Overflow: [25, 25, 25, 25]
	// Return nil (overflow handled internally, no parent update needed)
	if compareValues(firstKey, lastKey) == 0 {
		// Split at position 1, keeping first record in current page
		// All other records go to overflow block
		newBlk, err := ln.page.Split(1, flag)
		if err != nil {
			return nil, fmt.Errorf("failed to split: %w", err)
		}

		// Set flag to point to overflow block
		if err := ln.page.SetFlag(newBlk.Number()); err != nil {
			return nil, fmt.Errorf("failed to set flag: %w", err)
		}

		return nil, nil // No entry for parent (overflow handled internally)
	}

	// Keys are different, split in middle
	//
	// Example: Page full with [10, 20, 30, 35, 40] after Insert(35)
	// Different keys, so split: splitPos = 5/2 = 2, splitKey = 30
	// Split(2, -1): Current page: [10, 20], New page: [30, 35, 40]
	// Return InternalNodeEntry(30, newBlockNumber) for parent update
	splitPos := numRecs / 2
	splitKey, err := ln.page.GetVal(splitPos, "dataval")
	if err != nil {
		return nil, fmt.Errorf("failed to get split key: %w", err)
	}

	// Handle duplicate keys at split position
	// If split key equals first key, move right to find next different key
	//
	// Example: [10, 10, 10, 20, 30], splitPos=2, splitKey=10, firstKey=10
	// Since splitKey == firstKey, move right: pos 3 has key=20 (different)
	// Final: splitPos=3, splitKey=20 → Split: [10, 10, 10] | [20, 30]
	if compareValues(splitKey, firstKey) == 0 {
		for splitPos < numRecs {
			currentKey, err := ln.page.GetVal(splitPos, "dataval")
			if err != nil {
				return nil, fmt.Errorf("failed to get key: %w", err)
			}
			if compareValues(currentKey, splitKey) != 0 {
				splitKey = currentKey
				break
			}
			splitPos++
		}
	} else {
		// Move left to find first entry with this key
		//
		// Example: [10, 20, 25, 25, 30], splitPos=2, splitKey=25, firstKey=10
		// Since splitKey != firstKey, move left to find first 25: pos 2 is first 25
		// Final: splitPos=2, splitKey=25 → Split: [10, 20] | [25, 25, 30]
		for splitPos > 0 {
			prevKey, err := ln.page.GetVal(splitPos-1, "dataval")
			if err != nil {
				return nil, fmt.Errorf("failed to get key: %w", err)
			}
			if compareValues(prevKey, splitKey) != 0 {
				break
			}
			splitPos--
		}
	}

	// Split the page (flag = -1 means regular leaf, not overflow)
	//
	// Example result: Current page keeps records [0 to splitPos-1]
	// New page gets records [splitPos to end]
	// Parent gets InternalNodeEntry(splitKey, newBlockNumber) to route future searches
	newBlk, err := ln.page.Split(splitPos, -1)
	if err != nil {
		return nil, fmt.Errorf("failed to split: %w", err)
	}

	// Return entry for parent to update
	return NewInternalNodeEntry(splitKey, newBlk.Number()), nil
}

// Delete removes a record from the leaf node
// Uses Next() to iterate through matching records until we find the exact RID
func (ln *Leaf) Delete(dataRid *record.RID) error {
	for {
		hasNext, err := ln.Next()
		if err != nil {
			return fmt.Errorf("failed to get next: %w", err)
		}
		if !hasNext {
			return nil // Record not found
		}

		currentRid, err := ln.GetDataRid()
		if err != nil {
			return fmt.Errorf("failed to get RID: %w", err)
		}

		// Check if RIDs match
		if currentRid.Block() == dataRid.Block() && currentRid.Slot() == dataRid.Slot() {
			return ln.page.Delete(ln.currentSlot)
		}
	}
}
