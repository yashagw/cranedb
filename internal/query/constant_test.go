package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConstantBasic(t *testing.T) {
	// Test creating int constant
	intConst := NewIntConstant(42)
	require.NotNil(t, intConst)
	assert.Equal(t, 42, intConst.AsInt())
	assert.Equal(t, "42", intConst.String())

	// Test creating string constant
	strConst := NewStringConstant("hello")
	require.NotNil(t, strConst)
	assert.Equal(t, "hello", strConst.AsString())
	assert.Equal(t, "hello", strConst.String())

	// Test Equals - comparing same constant instance
	intConst1 := NewIntConstant(10)
	intConst2 := NewIntConstant(20)
	assert.True(t, intConst1.Equals(intConst1))  // same instance
	assert.False(t, intConst1.Equals(intConst2)) // different values

	// Test Equals with string constants
	strConst1 := NewStringConstant("test")
	strConst2 := NewStringConstant("different")
	assert.True(t, strConst1.Equals(strConst1))  // same instance
	assert.False(t, strConst1.Equals(strConst2)) // different values

	// Test Equals with different types
	assert.False(t, intConst1.Equals(strConst1))

	// Test CompareTo with int constants
	intConst3 := NewIntConstant(10)                     // same value as intConst1
	intConst4 := NewIntConstant(5)                      // less than intConst1
	assert.Equal(t, 0, intConst1.CompareTo(intConst3))  // equal values
	assert.Equal(t, -1, intConst4.CompareTo(intConst1)) // less than
	assert.Equal(t, 1, intConst2.CompareTo(intConst1))  // greater than

	// Test CompareTo with string constants
	strConstA := NewStringConstant("apple")
	strConstB := NewStringConstant("banana")
	strConstC := NewStringConstant("test")              // same as strConst1
	assert.Equal(t, -1, strConstA.CompareTo(strConstB)) // less than
	assert.Equal(t, 1, strConstB.CompareTo(strConstA))  // greater than
	assert.Equal(t, 0, strConst1.CompareTo(strConstC))  // equal values

	// Test CompareTo with different types
	assert.Equal(t, -1, intConst1.CompareTo(strConst1)) // types don't match
}
