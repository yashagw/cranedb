package scan

var (
	_ Scan = (*ProductScan)(nil)
)

type ProductScan struct {
	scan1 Scan
	scan2 Scan
}

func NewProductScan(scan1 Scan, scan2 Scan) *ProductScan {
	return &ProductScan{
		scan1: scan1,
		scan2: scan2,
	}
}

// BeforeFirst positions the scan before the first record in both scans.
// It prepares scan1 by moving it to the first record and positions scan2 before its first record.
func (s *ProductScan) BeforeFirst() {
	s.scan1.BeforeFirst()
	s.scan1.Next()
	s.scan2.BeforeFirst()
}

// Next moves to the next record in the product of scan1 and scan2.
// It tries to move scan2 to the next record; if successful, returns true.
// Otherwise, it resets scan2 and advances scan1. If scan1 has a next record,
// it advances scan2 to its next record and returns true.
// Returns false when the product is fully traversed.
func (s *ProductScan) Next() bool {
	if s.scan2.Next() {
		// There is another record in scan2 for the current scan1 record
		return true
	} else {
		// Reset scan2 to before the first so we can reuse for the next scan1 record
		s.scan2.BeforeFirst()
		return s.scan2.Next() && s.scan1.Next()
	}
}

func (s *ProductScan) GetInt(fldname string) int {
	if s.scan1.HasField(fldname) {
		return s.scan1.GetInt(fldname)
	}
	return s.scan2.GetInt(fldname)
}

func (s *ProductScan) GetString(fldname string) string {
	if s.scan1.HasField(fldname) {
		return s.scan1.GetString(fldname)
	}
	return s.scan2.GetString(fldname)
}

func (s *ProductScan) HasField(fldname string) bool {
	return s.scan1.HasField(fldname) || s.scan2.HasField(fldname)
}

func (s *ProductScan) Close() {
	s.scan1.Close()
	s.scan2.Close()
}
