package record

type FieldInfo struct {
	fieldLength int
	fieldType   string
}

type Schema struct {
	fields    []string
	fieldInfo map[string]FieldInfo
}

// NewSchema creates a new schema
func NewSchema() *Schema {
	return &Schema{
		fields:    make([]string, 0),
		fieldInfo: make(map[string]FieldInfo),
	}
}

func (s *Schema) AddField(name string, fieldType string, length int) {
	if _, exists := s.fieldInfo[name]; !exists {
		s.fields = append(s.fields, name)
	}
	s.fieldInfo[name] = FieldInfo{
		fieldLength: length,
		fieldType:   fieldType,
	}
}

func (s *Schema) AddIntField(name string) {
	s.AddField(name, "int", 4)
}

func (s *Schema) AddStringField(name string, length int) {
	s.AddField(name, "string", length)
}

func (s *Schema) Copy(other *Schema, fieldName string) {
	if info, exists := other.fieldInfo[fieldName]; exists {
		s.AddField(fieldName, info.fieldType, info.fieldLength)
	}
}

func (s *Schema) CopyAll(other *Schema) {
	for _, field := range other.fields {
		info := other.fieldInfo[field]
		s.AddField(field, info.fieldType, info.fieldLength)
	}
}
