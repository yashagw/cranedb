package transaction

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshot_OwnTransaction(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{3, 7}, 10)

	// Own transaction is always visible
	assert.True(t, snapshot.IsVisible(5))
}

func TestSnapshot_CommittedBeforeSnapshot(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{3, 7}, 10)

	// Transaction 1 is not in active set and < xmax → visible
	assert.True(t, snapshot.IsVisible(1))
	assert.True(t, snapshot.IsVisible(2))
	assert.True(t, snapshot.IsVisible(4))
}

func TestSnapshot_ActiveAtSnapshotTime(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{3, 7}, 10)

	// Transactions in active set are not visible
	assert.False(t, snapshot.IsVisible(3))
	assert.False(t, snapshot.IsVisible(7))
}

func TestSnapshot_FutureTransactions(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{3, 7}, 10)

	// Transactions >= xmax (nextTxNum) are from the future → not visible
	assert.False(t, snapshot.IsVisible(10))
	assert.False(t, snapshot.IsVisible(11))
	assert.False(t, snapshot.IsVisible(100))
}

func TestSnapshot_EmptyActiveSet(t *testing.T) {
	snapshot := NewSnapshot(5, []int64{}, 10)

	// With no active transactions, all past transactions are visible
	assert.True(t, snapshot.IsVisible(1))
	assert.True(t, snapshot.IsVisible(4))
	assert.True(t, snapshot.IsVisible(9))
	assert.False(t, snapshot.IsVisible(10)) // future
}

func TestSnapshot_TxNum(t *testing.T) {
	snapshot := NewSnapshot(42, []int64{1, 2}, 50)
	assert.Equal(t, int64(42), snapshot.TxNum())
}
