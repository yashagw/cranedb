package parserdata

type SetData struct {
	variableName string
	value        interface{} // can be bool, string, or int
}

func NewSetData(variableName string, value interface{}) *SetData {
	return &SetData{
		variableName: variableName,
		value:        value,
	}
}

func (s *SetData) VariableName() string {
	return s.variableName
}

func (s *SetData) Value() interface{} {
	return s.value
}
