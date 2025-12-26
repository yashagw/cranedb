package index

import (
	"fmt"

	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

// BTPage represents a generic B-tree page
type BTPage struct {
	tx         *transaction.Transaction
	currentBlk *file.BlockID
	layout     *record.Layout
}

// NewBTPage creates a new B-tree page for the specified block
func NewBTPage(tx *transaction.Transaction, blk *file.BlockID, layout *record.Layout) (*BTPage, error) {
	_, err := tx.Pin(blk)
	if err != nil {
		return nil, fmt.Errorf("failed to pin block %s: %w", blk.String(), err)
	}

	return &BTPage{
		tx:         tx,
		currentBlk: blk,
		layout:     layout,
	}, nil
}

// Close unpins the page from the buffer pool
func (bp *BTPage) Close() error {
	if bp.currentBlk != nil {
		bp.tx.Unpin(bp.currentBlk)
		bp.currentBlk = nil
	}
	return nil
}

// GetTransaction returns the transaction associated with this page
func (bp *BTPage) GetTransaction() *transaction.Transaction {
	return bp.tx
}

// GetLayout returns the layout associated with this page
func (bp *BTPage) GetLayout() *record.Layout {
	return bp.layout
}

// GetCurrentBlock returns the BlockID of the current page
func (bp *BTPage) GetCurrentBlock() *file.BlockID {
	return bp.currentBlk
}

// GetFlag returns the flag value stored at the beginning of the page
//
// Flag semantics depend on page type:
//   - Leaf pages (leaf table):
//   - flag = -1: Regular leaf page (no overflow)
//   - flag >= 0: Leaf page with overflow (flag contains overflow block number)
//   - Internal pages (directory table):
//   - flag >= 1: Internal node level (1 = points to leaf pages, 2+ = points to internal pages at level flag-1)
//
// IMPORTANT: The flag value has different meanings depending on which table the page is in:
//   - In leaf table: flag < 1 means leaf page (flag = -1 for no overflow, flag >= 0 for overflow block number)
//   - In directory table: flag >= 1 means internal page (flag = level number)
//   - The check "flag < 1" is used to distinguish leaves from internals when traversing
func (bp *BTPage) GetFlag() (int, error) {
	return bp.tx.GetInt(bp.currentBlk, 0)
}

// SetFlag sets the flag value at the beginning of the page
func (bp *BTPage) SetFlag(flag int) error {
	return bp.tx.SetInt(bp.currentBlk, 0, flag, true)
}

// IsLeafPage checks if a flag value represents a leaf page
// Leaf pages have flag < 1 (either -1 for no overflow, or >= 0 for overflow block number)
// This is used when traversing the tree to distinguish leaf pages from internal pages
func IsLeafPage(flag int) bool {
	return flag < 1
}

// IsInternalPage checks if a flag value represents an internal page
// Internal pages have flag >= 1 (flag = level number: 1 = points to leaves, 2+ = points to internals)
func IsInternalPage(flag int) bool {
	return flag >= 1
}

// GetInternalLevel returns the level of an internal page
// Returns the flag value (which is the level number) and an error if flag < 1
// Level 1 = points to leaf pages, Level 2+ = points to internal pages at level (flag-1)
func GetInternalLevel(flag int) (int, error) {
	if flag < 1 {
		return 0, fmt.Errorf("flag %d does not represent an internal page (expected >= 1)", flag)
	}
	return flag, nil
}

// IsLevel1Internal checks if an internal page is at level 1 (points directly to leaf pages)
// Level 1 internal pages have flag == 1
func IsLevel1Internal(flag int) bool {
	return flag == 1
}

// GetOverflowBlock returns the overflow block number for a leaf page
// Returns the flag value (which is the overflow block number) and an error if flag < 0
// flag = -1 means no overflow, flag >= 0 means overflow block number
func GetOverflowBlock(flag int) (int, error) {
	if flag < 0 {
		return 0, fmt.Errorf("flag %d does not represent a leaf with overflow (expected >= 0, -1 means no overflow)", flag)
	}
	return flag, nil
}

// HasOverflow checks if a leaf page has an overflow block
// Returns true if flag >= 0 (overflow exists), false if flag == -1 (no overflow)
func HasOverflow(flag int) bool {
	return flag >= 0
}

// GetNumRecs returns the number of records stored in this page
func (bp *BTPage) GetNumRecs() (int, error) {
	return bp.tx.GetInt(bp.currentBlk, 4) // 4 bytes after the flag
}

// SetNumRecs sets the number of records stored in this page
func (bp *BTPage) SetNumRecs(count int) error {
	return bp.tx.SetInt(bp.currentBlk, 4, count, true)
}

// Format initializes a new empty page with the specified flag
// This sets up the page header and creates empty slots for records
func (bp *BTPage) Format(flag int) error {
	// Set the page type flag
	if err := bp.SetFlag(flag); err != nil {
		return fmt.Errorf("failed to set flag: %w", err)
	}

	// Initialize record count to 0
	if err := bp.SetNumRecs(0); err != nil {
		return fmt.Errorf("failed to set record count: %w", err)
	}

	// Initialize all possible record slots with default values
	recSize := bp.layout.GetSlotSize()
	blockSize := bp.tx.BlockSize()

	// Start after the header (flag + numRecords = 8 bytes)
	dataAreaSize := blockSize - file.PageHeaderSize
	for pos := 8; pos+recSize <= dataAreaSize; pos += recSize {
		if err := bp.makeDefaultRecord(pos); err != nil {
			return fmt.Errorf("failed to create default record at pos %d: %w", pos, err)
		}
	}

	return nil
}

// makeDefaultRecord initializes a record at the given position with default values
func (bp *BTPage) makeDefaultRecord(pos int) error {
	schema := bp.layout.GetSchema()
	for _, fieldName := range schema.Fields() {
		fieldOffset := bp.layout.GetOffset(fieldName)
		totalOffset := pos + fieldOffset

		fieldType := schema.Type(fieldName)
		switch fieldType {
		case record.FieldTypeInt:
			if err := bp.tx.SetInt(bp.currentBlk, totalOffset, 0, false); err != nil {
				return err
			}
		case record.FieldTypeString:
			if err := bp.tx.SetString(bp.currentBlk, totalOffset, "", false); err != nil {
				return err
			}
		case record.FieldTypeBool:
			if err := bp.tx.SetBool(bp.currentBlk, totalOffset, false, false); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported field type: %s", fieldType)
		}
	}
	return nil
}

// GetSlotPosition calculates the byte position where a slot begins in the page
// Page layout: [Flag: 4 bytes][NumRecords: 4 bytes][Slot 0][Slot 1]...[Slot N]
func (bp *BTPage) GetSlotPosition(slot int) int {
	slotSize := bp.layout.GetSlotSize()
	headerSize := 8 // Flag (4 bytes) + NumRecords (4 bytes)
	return headerSize + (slot * slotSize)
}

// GetFieldPosition calculates the byte position of a specific field within a slot
func (bp *BTPage) GetFieldPosition(slot int, fieldName string) int {
	fieldOffset := bp.layout.GetOffset(fieldName)
	return bp.GetSlotPosition(slot) + fieldOffset
}

// GetInt reads an integer value from the specified slot and field
func (bp *BTPage) GetInt(slot int, fieldName string) (int, error) {
	pos := bp.GetFieldPosition(slot, fieldName)
	return bp.tx.GetInt(bp.currentBlk, pos)
}

// GetBool reads a boolean value from the specified slot and field
func (bp *BTPage) GetBool(slot int, fieldName string) (bool, error) {
	pos := bp.GetFieldPosition(slot, fieldName)
	return bp.tx.GetBool(bp.currentBlk, pos)
}

// GetString reads a string value from the specified slot and field
func (bp *BTPage) GetString(slot int, fieldName string) (string, error) {
	pos := bp.GetFieldPosition(slot, fieldName)
	return bp.tx.GetString(bp.currentBlk, pos)
}

// GetVal reads a value of any type from the specified slot and field
func (bp *BTPage) GetVal(slot int, fieldName string) (any, error) {
	fieldType := bp.layout.GetSchema().Type(fieldName)
	switch fieldType {
	case record.FieldTypeInt:
		return bp.GetInt(slot, fieldName)
	case record.FieldTypeString:
		return bp.GetString(slot, fieldName)
	case record.FieldTypeBool:
		return bp.GetBool(slot, fieldName)
	default:
		return nil, fmt.Errorf("unsupported field type: %s", fieldType)
	}
}

// SetInt writes an integer value to the specified slot and field
func (bp *BTPage) SetInt(slot int, fieldName string, val int) error {
	pos := bp.GetFieldPosition(slot, fieldName)
	return bp.tx.SetInt(bp.currentBlk, pos, val, true)
}

// SetBool writes an boolean value to the specified slot and field
func (bp *BTPage) SetBool(slot int, fieldName string, val bool) error {
	pos := bp.GetFieldPosition(slot, fieldName)
	return bp.tx.SetBool(bp.currentBlk, pos, val, true)
}

// SetString writes a string value to the specified slot and field
func (bp *BTPage) SetString(slot int, fieldName string, val string) error {
	pos := bp.GetFieldPosition(slot, fieldName)
	return bp.tx.SetString(bp.currentBlk, pos, val, true)
}

// SetVal writes a value of any type to the specified slot and field
func (bp *BTPage) SetVal(slot int, fieldName string, val any) error {
	fieldType := bp.layout.GetSchema().Type(fieldName)
	switch fieldType {
	case record.FieldTypeInt:
		if intVal, ok := val.(int); ok {
			return bp.SetInt(slot, fieldName, intVal)
		}
		return fmt.Errorf("expected int for field %s, got %T", fieldName, val)
	case record.FieldTypeBool:
		if boolVal, ok := val.(bool); ok {
			return bp.SetBool(slot, fieldName, boolVal)
		}
		return fmt.Errorf("expected bool for field %s, got %T", fieldName, val)
	case record.FieldTypeString:
		if strVal, ok := val.(string); ok {
			return bp.SetString(slot, fieldName, strVal)
		}
		return fmt.Errorf("expected string for field %s, got %T", fieldName, val)
	default:
		return fmt.Errorf("unsupported field type: %s", fieldType)
	}
}

// Insert creates space for a new record at the specified slot by shifting existing records to the right
// Before Insert(1):
// Slot 0: [block=10, id=20, dataval=30]
// Slot 1: [block=40, id=50, dataval=60]
// Slot 2: [block=70, id=80, dataval=90]
// Record Count: 3
// After Insert(1):
// Slot 0: [block=10, id=20, dataval=30]  ← unchanged
// Slot 1: [empty slot - ready for new data]
// Slot 2: [block=40, id=50, dataval=60]  ← shifted from slot 1
// Slot 3: [block=70, id=80, dataval=90]  ← shifted from slot 2
// Record Count: 4
func (bp *BTPage) Insert(slot int) error {
	numRecs, err := bp.GetNumRecs()
	if err != nil {
		return fmt.Errorf("failed to get record count: %w", err)
	}

	// Shift all records from slot onwards to the right
	for i := numRecs; i > slot; i-- {
		if err := bp.CopyRecord(i-1, i); err != nil {
			return fmt.Errorf("failed to copy record from %d to %d: %w", i-1, i, err)
		}
	}

	// Increment the record count
	return bp.SetNumRecs(numRecs + 1)
}

// Delete removes the record at the specified slot by shifting remaining records to the left
// Before Delete(1):
// Slot 0: [block=10, id=20, dataval=30]
// Slot 1: [block=40, id=50, dataval=60]  ← to be deleted
// Slot 2: [block=70, id=80, dataval=90]
// Slot 3: [block=100, id=110, dataval=120]
// Record Count: 4
// After Delete(1):
// Slot 0: [block=10, id=20, dataval=30]   ← unchanged
// Slot 1: [block=70, id=80, dataval=90]   ← shifted from slot 2
// Slot 2: [block=100, id=110, dataval=120] ← shifted from slot 3
// Slot 3: [unused]                        ← no longer part of records
// Record Count: 3
func (bp *BTPage) Delete(slot int) error {
	numRecs, err := bp.GetNumRecs()
	if err != nil {
		return fmt.Errorf("failed to get record count: %w", err)
	}

	// Shift all records after slot to the left
	for i := slot + 1; i < numRecs; i++ {
		if err := bp.CopyRecord(i, i-1); err != nil {
			return fmt.Errorf("failed to copy record from %d to %d: %w", i, i-1, err)
		}
	}

	// Decrement the record count
	return bp.SetNumRecs(numRecs - 1)
}

// CopyRecord copies all fields from one slot to another
func (bp *BTPage) CopyRecord(from, to int) error {
	schema := bp.layout.GetSchema()
	for _, fieldName := range schema.Fields() {
		val, err := bp.GetVal(from, fieldName)
		if err != nil {
			return fmt.Errorf("failed to get value from slot %d, field %s: %w", from, fieldName, err)
		}
		if err := bp.SetVal(to, fieldName, val); err != nil {
			return fmt.Errorf("failed to set value to slot %d, field %s: %w", to, fieldName, err)
		}
	}
	return nil
}

// FindSlotBefore finds the slot position before the first record with dataval >= searchKey
// Returns the slot number where the searchKey should be inserted to maintain sorted order
//
// Example with page containing sorted keys [10, 20, 30, 50, 70]:
// FindSlotBefore(5)  → returns -1 (insert at beginning, before slot 0)
// FindSlotBefore(10) → returns -1 (insert at beginning, key equals first record)
// FindSlotBefore(15) → returns 0  (insert after slot 0, before slot 1)
// FindSlotBefore(25) → returns 1  (insert after slot 1, before slot 2)
// FindSlotBefore(30) → returns 1  (insert after slot 1, key equals record at slot 2)
// FindSlotBefore(40) → returns 2  (insert after slot 2, before slot 3)
// FindSlotBefore(70) → returns 3  (insert after slot 3, key equals last record)
// FindSlotBefore(80) → returns 4  (insert at end, after slot 4)
//
// The returned slot indicates: "insert AFTER this slot to maintain sort order"
// A return value of -1 means: "insert at the very beginning (slot 0)"
func (bp *BTPage) FindSlotBefore(searchKey any) (int, error) {
	numRecs, err := bp.GetNumRecs()
	if err != nil {
		return 0, fmt.Errorf("failed to get record count: %w", err)
	}

	slot := 0
	for slot < numRecs {
		dataVal, err := bp.GetVal(slot, "dataval")
		if err != nil {
			return 0, fmt.Errorf("failed to get dataval at slot %d: %w", slot, err)
		}

		// return -1 if dataVal is less than searchKey
		// return 0 if dataVal is equal to searchKey
		// return 1 if dataVal is greater than searchKey
		if compareValues(dataVal, searchKey) >= 0 {
			break
		}
		slot++
	}

	return slot - 1, nil
}

// IsFull checks if the page can accommodate one more record
func (bp *BTPage) IsFull() (bool, error) {
	numRecs, err := bp.GetNumRecs()
	if err != nil {
		return false, fmt.Errorf("failed to get record count: %w", err)
	}

	// Calculate position where the next record would go
	nextSlotPos := bp.GetSlotPosition(numRecs + 1)
	dataAreaSize := bp.tx.BlockSize() - file.PageHeaderSize

	return nextSlotPos > dataAreaSize, nil
}

// Split splits the page at the specified position and returns the new block
// Records from splitPos onwards are moved to the new page
//
// splitPos is zero-indexed: 0=first record, 1=second record, etc.
//
// Example: Split(3, 0) on a page with 6 records [A, B, C, D, E, F]:
// Before Split:
//
//	Original Page: [A, B, C, D, E, F] (6 records)
//	              slot: 0  1  2  3  4  5
//	Record Count: 6
//
// After Split(3, 0):  (splitPos=3 means split starting at slot 3)
//
//	Original Page: [A, B, C] (3 records, slots 0-2 remain)
//	New Page:      [D, E, F] (3 records, slots 3-5 moved to slots 0-2)
//	Record Count:  3 each
//
// The split creates a new block in the same file, formats it with the specified flag,
// and moves records from splitPos onwards to maintain B-tree structure.
// This is the core mechanism for B-tree growth when nodes become full.
func (bp *BTPage) Split(splitPos int, flag int) (*file.BlockID, error) {
	// Create a new block
	newBlk, err := bp.appendNew(flag)
	if err != nil {
		return nil, fmt.Errorf("failed to create new block: %w", err)
	}

	// Create a new BTPage for the new block
	newPage, err := NewBTPage(bp.tx, newBlk, bp.layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create new BTPage: %w", err)
	}
	defer newPage.Close()

	// Transfer records from splitPos onwards to the new page
	if err := bp.TransferRecs(splitPos, newPage); err != nil {
		return nil, fmt.Errorf("failed to transfer records: %w", err)
	}

	// Set the flag for the new page
	if err := newPage.SetFlag(flag); err != nil {
		return nil, fmt.Errorf("failed to set flag on new page: %w", err)
	}

	return newBlk, nil
}

// TransferRecs moves records from startSlot onwards to the destination page
//
// startSlot is zero-indexed: 0=first record, 1=second record, etc.
//
// Example: TransferRecs(2, destPage) on source page with records [A, B, C, D, E]:
// Before Transfer:
//
//	Source Page: [A, B, C, D, E] (5 records)
//	            slot: 0  1  2  3  4
//	Dest Page:   [] (0 records)
//
// After Transfer(2):  (startSlot=2 means transfer starting at slot 2)
//
//	Source Page: [A, B] (2 records, slots 0-1 remain)
//	Dest Page:   [C, D, E] (3 records, slots 2-4 moved to slots 0-2)
//
// Process:
// 1. Copy records from startSlot onwards to destination (field by field)
// 2. Delete the transferred records from source page
// 3. Destination gets records at slots 0, 1, 2... (compacted)
// 4. Source page shrinks and updates record count
//
// Used by Split() to move records between pages during node splitting.
func (bp *BTPage) TransferRecs(startSlot int, destPage *BTPage) error {
	numRecs, err := bp.GetNumRecs()
	if err != nil {
		return fmt.Errorf("failed to get record count: %w", err)
	}

	destSlot := 0
	for slot := startSlot; slot < numRecs; slot++ {
		// Insert a new record in the destination page
		if err := destPage.Insert(destSlot); err != nil {
			return fmt.Errorf("failed to insert record at dest slot %d: %w", destSlot, err)
		}

		// Copy all fields from source to destination
		schema := bp.layout.GetSchema()
		for _, fieldName := range schema.Fields() {
			val, err := bp.GetVal(slot, fieldName)
			if err != nil {
				return fmt.Errorf("failed to get value from slot %d, field %s: %w", slot, fieldName, err)
			}
			if err := destPage.SetVal(destSlot, fieldName, val); err != nil {
				return fmt.Errorf("failed to set value to dest slot %d, field %s: %w", destSlot, fieldName, err)
			}
		}

		destSlot++
	}

	// Remove the transferred records from the source page
	for slot := startSlot; slot < numRecs; slot++ {
		if err := bp.Delete(startSlot); err != nil {
			return fmt.Errorf("failed to delete record at slot %d: %w", startSlot, err)
		}
	}

	return nil
}

// appendNew creates a new block in the same file and formats it
//
// Example: appendNew(0) when current page is in file "btree_index.tbl":
// Before:
//
//	File "btree_index.tbl" has blocks: [0, 1, 2] (3 blocks)
//	Current page is at block 1
//
// After:
//
//	File "btree_index.tbl" has blocks: [0, 1, 2, 3] (4 blocks)
//	Returns: BlockID{filename: "btree_index.tbl", number: 3}
//	New block 3 is formatted with:
//	  - Flag = 0 (leaf node)
//	  - NumRecords = 0
//	  - All record slots initialized with default values
//
// Process:
// 1. Get filename from current block
// 2. Append new block to same file (gets next block number)
// 3. Pin the new block in buffer pool
// 4. Format it as empty B-tree page with specified flag
// 5. Unpin and return the new BlockID
//
// Used by Split() to create destination pages for record transfers.
func (bp *BTPage) appendNew(flag int) (*file.BlockID, error) {
	filename := bp.currentBlk.Filename()
	newBlk, err := bp.tx.Append(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to append new block: %w", err)
	}

	// Create a temporary BTPage to format the new block
	tempPage, err := NewBTPage(bp.tx, newBlk, bp.layout)
	if err != nil {
		return nil, fmt.Errorf("failed to create temp page: %w", err)
	}

	if err := tempPage.Format(flag); err != nil {
		tempPage.Close() // This will unpin
		return nil, fmt.Errorf("failed to format new block: %w", err)
	}

	tempPage.Close() // Unpin the temp page
	return newBlk, nil
}

// compareValues compares two values and returns:
// -1 if a < b, 0 if a == b, 1 if a > b
func compareValues(a, b any) int {
	switch aVal := a.(type) {
	case int:
		if bVal, ok := b.(int); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	case string:
		if bVal, ok := b.(string); ok {
			if aVal < bVal {
				return -1
			} else if aVal > bVal {
				return 1
			}
			return 0
		}
	}
	return 0 // Default to equal if types don't match
}

// InsertInternalNode inserts a new entry in an internal node page
func (bp *BTPage) InsertInternalNode(slot int, val any, blockNum int) error {
	if err := bp.Insert(slot); err != nil {
		return fmt.Errorf("failed to insert slot: %w", err)
	}
	if err := bp.SetVal(slot, "dataval", val); err != nil {
		return fmt.Errorf("failed to set dataval: %w", err)
	}
	if err := bp.SetInt(slot, "block", blockNum); err != nil {
		return fmt.Errorf("failed to set block number: %w", err)
	}
	// Note: "id" field is unused in internal nodes, but we don't need to set it
	// as it's already initialized to 0 by Format()
	return nil
}

// InsertLeaf inserts a new entry in a leaf node page
func (bp *BTPage) InsertLeaf(slot int, val any, rid *record.RID) error {
	if err := bp.Insert(slot); err != nil {
		return fmt.Errorf("failed to insert slot: %w", err)
	}
	if err := bp.SetVal(slot, "dataval", val); err != nil {
		return fmt.Errorf("failed to set dataval: %w", err)
	}
	if err := bp.SetInt(slot, "block", rid.Block()); err != nil {
		return fmt.Errorf("failed to set block number: %w", err)
	}
	if err := bp.SetInt(slot, "id", rid.Slot()); err != nil {
		return fmt.Errorf("failed to set slot number: %w", err)
	}
	return nil
}

// GetBlockNum returns the block number at the specified slot
// Used for INTERNAL nodes: returns the child block number
func (bp *BTPage) GetBlockNum(slot int) (int, error) {
	return bp.GetInt(slot, "block")
}

// GetChildBlockNum is an alias for GetBlockNum
// Returns the child block number for internal node entries
func (bp *BTPage) GetChildBlockNum(slot int) (int, error) {
	return bp.GetBlockNum(slot)
}

// GetDataRid returns the record identifier (RID) at the specified slot
// Used for LEAF nodes: combines "block" and "id" fields to form a RID
// Returns a RID pointing to the actual data record in the indexed table
func (bp *BTPage) GetDataRid(slot int) (*record.RID, error) {
	blockNum, err := bp.GetInt(slot, "block")
	if err != nil {
		return nil, fmt.Errorf("failed to get block number: %w", err)
	}
	slotNum, err := bp.GetInt(slot, "id")
	if err != nil {
		return nil, fmt.Errorf("failed to get slot number: %w", err)
	}
	return record.NewRID(blockNum, slotNum), nil
}
