package record

const (
	SlotFlagSize   = 4                                  // 4 bytes for empty/in-use flag
	XminSize       = 8                                  // 8 bytes for xmin (creating transaction ID)
	XmaxSize       = 8                                  // 8 bytes for xmax (deleting transaction ID)
	MVCCHeaderSize = SlotFlagSize + XminSize + XmaxSize // 20 bytes total

	XminOffset = SlotFlagSize            // 4
	XmaxOffset = SlotFlagSize + XminSize // 12
)

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

// NewLayoutFromSchema creates a new layout from a schema
func NewLayoutFromSchema(schema *Schema) *Layout {
	offsets := make(map[string]int)
	pos := MVCCHeaderSize
	for _, field := range schema.fields {
		offsets[field] = pos
		fieldInfo := schema.fieldInfo[field]
		if fieldInfo.fieldType == FieldTypeInt {
			pos += 4
		} else if fieldInfo.fieldType == FieldTypeString {
			// String needs 4 bytes for length prefix + fieldLength for data
			pos += 4 + fieldInfo.fieldLength
		} else if fieldInfo.fieldType == FieldTypeBool {
			pos += 1
		} else {
			// Fallback to fieldLength for unknown types
			pos += fieldInfo.fieldLength
		}
	}
	slotSize := pos

	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

// NewLayout creates a new layout from a schema and offsets
func NewLayout(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *Layout) GetOffset(fieldName string) int {
	return l.offsets[fieldName]
}

func (l *Layout) GetSlotSize() int {
	return l.slotSize
}

// GetSchema returns the schema associated with this layout
func (l *Layout) GetSchema() *Schema {
	return l.schema
}

func (l *Layout) lengthInBytes(fieldName string) int {
	fieldInfo, ok := l.schema.fieldInfo[fieldName]
	if !ok {
		// Consider panicking here
		return 0
	}

	if fieldInfo.fieldType == FieldTypeInt {
		return 4
	} else if fieldInfo.fieldType == FieldTypeString {
		// Assume string's length field tells max bytes for storage, plus 4 bytes prefix for VARCHAR length
		// Adjust depending on your actual Page & encoding logic
		return 4 + fieldInfo.fieldLength
	} else if fieldInfo.fieldType == FieldTypeBool {
		return 1
	}

	return 0
}
