package transaction

import "errors"

var ErrWriteConflict = errors.New("write-write conflict: another transaction modified this row")

// CheckWriteConflict checks if the current transaction can write to a tuple.
// First-writer-wins: if another transaction has set xmax on this tuple,
// the current transaction gets a conflict error.
func CheckWriteConflict(
	xmax int64,
	currentTxNum int64,
	commitLog *CommitLog,
) error {
	if xmax == 0 {
		return nil
	}
	if xmax == currentTxNum {
		return nil
	}
	return ErrWriteConflict
}
