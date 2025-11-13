package scan

type Index interface {
}

var (
	_ Scan = (*IndexSelectScan)(nil)
)

type IndexSelectScan struct {
	input Scan
	index Index
	value any
}

func NewIndexSelectScan(input Scan, index Index, value any) *IndexSelectScan {
	return &IndexSelectScan{
		input: input,
		index: index,
		value: value,
	}
}

func (iss *IndexSelectScan) BeforeFirst() error {
	return nil
}

func (is *IndexSelectScan) Next() (bool, error) {
	return false, nil
}

func (is *IndexSelectScan) GetInt(fldname string) (int, error) {
	return 0, nil
}

func (is *IndexSelectScan) GetString(fldname string) (string, error) {
	return "", nil
}

func (is *IndexSelectScan) GetValue(fldname string) (any, error) {
	return nil, nil
}

func (is *IndexSelectScan) HasField(fldname string) bool {
	return false
}

func (is *IndexSelectScan) Close() {
	is.input.Close()
}
