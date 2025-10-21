package record

import (
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/transaction"
)

type SlotStatus int

const (
	SlotStatusEmpty SlotStatus = 0
	SlotStatusInUse SlotStatus = 1
)

type RecordPage struct {
	transaction *transaction.Transaction
	block       *file.BlockID
	layout      *Layout
}

// NewRecordPage creates a new record page for the given transaction, block, and layout.
func NewRecordPage(transaction *transaction.Transaction, block *file.BlockID, layout *Layout) *RecordPage {
	// Pin the block to buffer pool
	transaction.Pin(block)

	// Create a new record page
	return &RecordPage{
		transaction: transaction,
		block:       block,
		layout:      NewLayoutFromSchema(layout.schema),
	}
}

// GetInt retrieves the integer value stored in the specified slot and field.
// Block Offset -> Where the slot starts (slot * layout.GetSlotSize()) + Where the field starts in the slot (layout.GetOffset(fieldName))
func (rp *RecordPage) GetInt(slot int, fieldName string) int {
	fieldOffset := rp.layout.GetOffset(fieldName)
	slotOffset := slot * rp.layout.GetSlotSize()
	totalOffset := fieldOffset + slotOffset
	return rp.transaction.GetInt(rp.block, totalOffset)
}

// GetString retrieves the string value stored in the specified slot and field.
func (rp *RecordPage) GetString(slot int, fieldName string) string {
	fieldOffset := rp.layout.GetOffset(fieldName)
	slotOffset := slot * rp.layout.GetSlotSize()
	totalOffset := fieldOffset + slotOffset
	return rp.transaction.GetString(rp.block, totalOffset)
}

// SetInt sets the integer value in the specified slot and field.
func (rp *RecordPage) SetInt(slot int, fieldName string, value int) {
	fieldOffset := rp.layout.GetOffset(fieldName)
	slotOffset := slot * rp.layout.GetSlotSize()
	totalOffset := fieldOffset + slotOffset
	rp.transaction.SetInt(rp.block, totalOffset, value, true)
}

// SetString sets the string value in the specified slot and field.
func (rp *RecordPage) SetString(slot int, fieldName string, value string) {
	fieldOffset := rp.layout.GetOffset(fieldName)
	slotOffset := slot * rp.layout.GetSlotSize()
	totalOffset := fieldOffset + slotOffset
	rp.transaction.SetString(rp.block, totalOffset, value, true)
}

func (rp *RecordPage) Delete(slot int) {
	rp.setSlotStatus(slot, SlotStatusEmpty)
}

// NextUsedSlot returns the index of the next slot after the given slot that is marked as USED.
// If no such slot is found, it returns -1.
func (rp *RecordPage) NextUsedSlot(slot int) int {
	return rp.searchAfter(slot, SlotStatusInUse)
}

// InsertSlot finds the next EMPTY slot after the given slot index, marks it as USED, and returns its index.
// If no empty slot is found, it returns -1.
func (rp *RecordPage) InsertSlot(slot int) int {
	newSlot := rp.searchAfter(slot, SlotStatusEmpty)
	if newSlot >= 0 {
		rp.setSlotStatus(newSlot, SlotStatusInUse)
	}
	return newSlot
}

// searchAfter finds and returns the first slot after the given slot index that matches the provided status.
// If no matching slot is found, it returns -1.
func (rp *RecordPage) searchAfter(slot int, status SlotStatus) int {
	slot++
	for rp.isValidSlot(slot) {
		slotOffset := slot * rp.layout.GetSlotSize()
		currStatus := SlotStatus(rp.transaction.GetInt(rp.block, slotOffset))
		if currStatus == status {
			return slot
		}
		slot++
	}
	return -1
}

// Format initializes all slots in the record page by setting them to empty status
// and initializing all fields with default values (0 for integers, empty string for strings).
func (rp *RecordPage) Format() {
	slot := 0
	for rp.isValidSlot(slot) {
		rp.setSlotStatus(slot, SlotStatusEmpty)
		schema := rp.layout.schema
		for _, fieldName := range schema.Fields() {
			fieldInfo, exists := schema.GetFieldInfo(fieldName)
			if !exists {
				continue
			}
			if fieldInfo.fieldType == "int" {
				rp.SetInt(slot, fieldName, 0)
			} else if fieldInfo.fieldType == "string" {
				rp.SetString(slot, fieldName, "")
			}
		}
		slot++
	}
}

func (rp *RecordPage) isValidSlot(slot int) bool {
	slotOffset := (slot + 1) * rp.layout.GetSlotSize()
	return slotOffset <= rp.transaction.BlockSize()
}

func (rp *RecordPage) getSlotStatus(slot int) SlotStatus {
	slotOffset := slot * rp.layout.GetSlotSize()
	totalOffset := slotOffset
	return SlotStatus(rp.transaction.GetInt(rp.block, totalOffset))
}

func (rp *RecordPage) setSlotStatus(slot int, status SlotStatus) {
	slotOffset := slot * rp.layout.GetSlotSize()
	totalOffset := slotOffset
	rp.transaction.SetInt(rp.block, totalOffset, int(status), true)
}

// Block returns the BlockID associated with this record page.
func (rp *RecordPage) Block() *file.BlockID {
	return rp.block
}
