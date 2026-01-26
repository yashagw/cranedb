package transaction

// IsVersionVisible determines if a tuple version (identified by its xmin/xmax)
// is visible to the given transaction's snapshot.
//
// A tuple is visible if:
//  1. xmin is committed (or is the current transaction) AND visible in snapshot
//  2. AND the tuple has not been deleted in a way visible to us
func IsVersionVisible(
	xmin int64,
	xmax int64,
	snapshot *Snapshot,
	commitLog *CommitLog,
	currentTxNum int64,
) bool {
	// Step 1: Check if the creating transaction's changes are visible
	xminVisible := false
	if xmin == currentTxNum {
		// We created this version
		if xmax == currentTxNum {
			// We also deleted it
			return false
		}
		xminVisible = true
	} else if commitLog.IsCommitted(xmin) && snapshot.IsVisible(xmin) {
		xminVisible = true
	}

	if !xminVisible {
		return false
	}

	// Step 2: Check if the version has been deleted
	if xmax == 0 {
		return true // Not deleted
	}

	if xmax == currentTxNum {
		return false // We deleted it
	}

	if !commitLog.IsCommitted(xmax) {
		return true // Deleting transaction hasn't committed
	}

	if !snapshot.IsVisible(xmax) {
		return true // Deleting transaction committed after our snapshot
	}

	// Deleting transaction committed and is visible to us
	return false
}
