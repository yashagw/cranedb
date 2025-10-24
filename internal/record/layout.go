package record

type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

// NewLayoutFromSchema creates a new layout from a schema
func NewLayoutFromSchema(schema *Schema) *Layout {
	offsets := make(map[string]int)
	pos := 4 // 4 bytes for the empty/inuse flag
	for _, field := range schema.fields {
		offsets[field] = pos
		pos += schema.fieldInfo[field].fieldLength
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
		return 0 // or consider panicking or returning an error
	}

	if fieldInfo.fieldType == "int" {
		return 4
	} else if fieldInfo.fieldType == "string" {
		// Assume string's length field tells max bytes for storage, plus 4 bytes prefix for VARCHAR length
		// Adjust depending on your actual Page & encoding logic
		return 4 + fieldInfo.fieldLength
	}
	return 0
}
