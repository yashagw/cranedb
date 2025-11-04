package scan

type Predicate struct {
}

func (p *Predicate) IsSatisfied(s Scan) bool {
	return false
}
