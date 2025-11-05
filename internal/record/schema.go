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

// Fields returns a copy of the field names slice
func (s *Schema) Fields() []string {
	fields := make([]string, len(s.fields))
	copy(fields, s.fields)
	return fields
}

// GetFieldInfo returns the field information for a given field name
func (s *Schema) GetFieldInfo(fieldName string) (FieldInfo, bool) {
	info, exists := s.fieldInfo[fieldName]
	return info, exists
}

// Type returns the type of a field
func (s *Schema) Type(fieldName string) string {
	if info, exists := s.fieldInfo[fieldName]; exists {
		return info.fieldType
	}
	return ""
}

// Length returns the length of a field
func (s *Schema) Length(fieldName string) int {
	if info, exists := s.fieldInfo[fieldName]; exists {
		return info.fieldLength
	}
	return 0
}

// HasField checks if the schema contains the specified field.
func (s *Schema) HasField(fieldName string) bool {
	_, exists := s.fieldInfo[fieldName]
	return exists
}
