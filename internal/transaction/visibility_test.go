package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVisibility_OwnInsertNotDeleted(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()

	// xmin=5 (our tx), xmax=0 (not deleted) → visible
	assert.True(t, IsVersionVisible(5, 0, snapshot, cl, 5))
}

func TestVisibility_OwnInsertOwnDelete(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()

	// xmin=5 (our tx), xmax=5 (we deleted) → not visible
	assert.False(t, IsVersionVisible(5, 5, snapshot, cl, 5))
}

func TestVisibility_CommittedInsertNotDeleted(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(3)

	// xmin=3 (committed, visible in snapshot), xmax=0 → visible
	assert.True(t, IsVersionVisible(3, 0, snapshot, cl, 5))
}

func TestVisibility_UncommittedInsert(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()

	// xmin=3 (NOT committed), xmax=0 → not visible
	assert.False(t, IsVersionVisible(3, 0, snapshot, cl, 5))
}

func TestVisibility_InsertByActiveTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{3}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(3)

	// xmin=3 (committed but was active in our snapshot) → not visible
	assert.False(t, IsVersionVisible(3, 0, snapshot, cl, 5))
}

func TestVisibility_InsertByFutureTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(12)

	// xmin=12 (future, >= snapshot.xmax) → not visible
	assert.False(t, IsVersionVisible(12, 0, snapshot, cl, 5))
}

func TestVisibility_DeletedByUncommittedTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(2)

	// xmin=2 (committed+visible), xmax=4 (not committed) → visible
	assert.True(t, IsVersionVisible(2, 4, snapshot, cl, 5))
}

func TestVisibility_DeletedByCommittedVisibleTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(2)
	cl.MarkCommitted(3)

	// xmin=2 (committed+visible), xmax=3 (committed+visible) → not visible
	assert.False(t, IsVersionVisible(2, 3, snapshot, cl, 5))
}

func TestVisibility_DeletedByCommittedButInvisibleTransaction(t *testing.T) {
	// Transaction 7 committed AFTER our snapshot was taken
	snapshot := NewSnapshot(5, []int64{7}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(2)
	cl.MarkCommitted(7)

	// xmin=2 (committed+visible), xmax=7 (committed but in active set → invisible) → visible
	assert.True(t, IsVersionVisible(2, 7, snapshot, cl, 5))
}

func TestVisibility_DeletedByOurTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(2)

	// xmin=2 (committed+visible), xmax=5 (our tx) → not visible (we deleted it)
	assert.True(t, IsVersionVisible(2, 0, snapshot, cl, 5))
	assert.False(t, IsVersionVisible(2, 5, snapshot, cl, 5))
}

func TestVisibility_DeletedByFutureTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)
	cl := NewCommitLog()
	cl.MarkCommitted(2)
	cl.MarkCommitted(12)

	// xmin=2 (committed+visible), xmax=12 (future, invisible in snapshot) → visible
	assert.True(t, IsVersionVisible(2, 12, snapshot, cl, 5))
}
