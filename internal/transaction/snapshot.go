package transaction

// Snapshot represents a point-in-time view of which transactions
// were active when this snapshot was taken.
type Snapshot struct {
	txNum     int64
	activeSet map[int64]bool
	xmax      int64 // next txNum that will be allocated (upper bound)
}

// NewSnapshot creates a snapshot for txNum given a list of active transaction IDs.
func NewSnapshot(txNum int64, activeTxIDs []int64, nextTxNum int64) *Snapshot {
	activeSet := make(map[int64]bool, len(activeTxIDs))
	for _, id := range activeTxIDs {
		activeSet[id] = true
	}
	return &Snapshot{
		txNum:     txNum,
		activeSet: activeSet,
		xmax:      nextTxNum,
	}
}

// IsVisible returns true if txID's changes should be visible to the snapshot owner.
func (s *Snapshot) IsVisible(txID int64) bool {
	if txID == s.txNum {
		return true
	}
	if txID >= s.xmax {
		return false
	}
	if s.activeSet[txID] {
		return false
	}
	return true
}

// TxNum returns the transaction ID that owns this snapshot.
func (s *Snapshot) TxNum() int64 {
	return s.txNum
}
