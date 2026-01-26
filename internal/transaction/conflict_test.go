package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckWriteConflict_NoExistingXmax(t *testing.T) {
	cl := NewCommitLog()

	// xmax=0 means no one has modified this tuple → no conflict
	err := CheckWriteConflict(0, 5, cl)
	assert.NoError(t, err)
}

func TestCheckWriteConflict_OwnXmax(t *testing.T) {
	cl := NewCommitLog()

	// xmax=5 set by our own transaction → no conflict
	err := CheckWriteConflict(5, 5, cl)
	assert.NoError(t, err)
}

func TestCheckWriteConflict_AnotherTransactionSetXmax(t *testing.T) {
	cl := NewCommitLog()

	// xmax=3 set by another transaction → conflict (first-writer-wins)
	err := CheckWriteConflict(3, 5, cl)
	require.Error(t, err)
	assert.Equal(t, ErrWriteConflict, err)
}

func TestCheckWriteConflict_AnotherCommittedTransaction(t *testing.T) {
	cl := NewCommitLog()
	cl.MarkCommitted(3)

	// xmax=3 set by committed transaction → conflict
	err := CheckWriteConflict(3, 5, cl)
	require.Error(t, err)
	assert.Equal(t, ErrWriteConflict, err)
}

func TestCheckWriteConflict_AnotherRunningTransaction(t *testing.T) {
	cl := NewCommitLog()

	// xmax=7 set by still-running transaction → conflict (first-writer-wins)
	err := CheckWriteConflict(7, 5, cl)
	require.Error(t, err)
	assert.Equal(t, ErrWriteConflict, err)
}
