package record

type RID struct {
	block int
	slot  int
}

func NewRID(block int, slot int) *RID {
	return &RID{
		block: block,
		slot:  slot,
	}
}

func (r *RID) Block() int {
	return r.block
}

func (r *RID) Slot() int {
	return r.slot
}
