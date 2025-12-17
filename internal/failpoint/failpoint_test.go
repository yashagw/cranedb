package failpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFailpointImmediateTrigger(t *testing.T) {
	ClearAll()
	defer ClearAll()

	Enable("test-failpoint", "")

	triggered := false
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})

	assert.True(t, triggered, "Failpoint should trigger immediately when value is empty")
	assert.Equal(t, 1, GetCount("test-failpoint"))
}

func TestFailpointWithCounter(t *testing.T) {
	ClearAll()
	defer ClearAll()

	// Enable failpoint with count=3 (trigger after 3 calls)
	Enable("test-failpoint", "3")

	triggered := false

	// First call - should not trigger
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})
	assert.False(t, triggered, "Failpoint should not trigger on first call")
	assert.Equal(t, 1, GetCount("test-failpoint"))

	// Second call - should not trigger
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})
	assert.False(t, triggered, "Failpoint should not trigger on second call")
	assert.Equal(t, 2, GetCount("test-failpoint"))

	// Third call - should trigger
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})
	assert.True(t, triggered, "Failpoint should trigger on third call")
	assert.Equal(t, 3, GetCount("test-failpoint"))
}

func TestFailpointWithCountFormat(t *testing.T) {
	ClearAll()
	defer ClearAll()

	// Enable failpoint with count=2 format
	Enable("test-failpoint", "count=2")

	triggered := false

	// First call - should not trigger
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})
	assert.False(t, triggered, "Failpoint should not trigger on first call")
	assert.Equal(t, 1, GetCount("test-failpoint"))

	// Second call - should trigger
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})
	assert.True(t, triggered, "Failpoint should trigger on second call")
	assert.Equal(t, 2, GetCount("test-failpoint"))
}

func TestFailpointDisableResetsCounter(t *testing.T) {
	ClearAll()
	defer ClearAll()

	Enable("test-failpoint", "2")

	// Call once
	Inject("test-failpoint", nil, nil)
	assert.Equal(t, 1, GetCount("test-failpoint"))

	// Disable and re-enable
	Disable("test-failpoint")
	Enable("test-failpoint", "2")

	// Counter should be reset
	assert.Equal(t, 0, GetCount("test-failpoint"))

	// Call once - should not trigger
	triggered := false
	Inject("test-failpoint", nil, func(val string, ctx interface{}) {
		triggered = true
	})
	assert.False(t, triggered, "Failpoint should not trigger after reset")
	assert.Equal(t, 1, GetCount("test-failpoint"))
}

func TestFailpointNotEnabled(t *testing.T) {
	ClearAll()
	defer ClearAll()

	triggered := false
	Inject("non-existent", nil, func(val string, ctx interface{}) {
		triggered = true
	})

	assert.False(t, triggered, "Failpoint should not trigger if not enabled")
	assert.Equal(t, 0, GetCount("non-existent"))
}

func TestFailpointMultipleFailpoints(t *testing.T) {
	ClearAll()
	defer ClearAll()

	Enable("fp1", "2")
	Enable("fp2", "3")

	triggered1 := false
	triggered2 := false

	// Call fp1 once
	Inject("fp1", nil, func(val string, ctx interface{}) {
		triggered1 = true
	})
	assert.False(t, triggered1)
	assert.Equal(t, 1, GetCount("fp1"))
	assert.Equal(t, 0, GetCount("fp2"))

	// Call fp2 twice
	Inject("fp2", nil, func(val string, ctx interface{}) {
		triggered2 = true
	})
	Inject("fp2", nil, func(val string, ctx interface{}) {
		triggered2 = true
	})
	assert.False(t, triggered2)
	assert.Equal(t, 1, GetCount("fp1"))
	assert.Equal(t, 2, GetCount("fp2"))

	// Call fp1 again - should trigger
	Inject("fp1", nil, func(val string, ctx interface{}) {
		triggered1 = true
	})
	assert.True(t, triggered1)
	assert.Equal(t, 2, GetCount("fp1"))
	assert.Equal(t, 2, GetCount("fp2"))
}

func TestFailpointContextMatching(t *testing.T) {
	ClearAll()
	defer ClearAll()

	Enable("fp-files", "file=target.tbl")

	triggered := false
	callback := func(val string, ctx interface{}) {
		triggered = true
	}

	// 1. Mismatch context
	Inject("fp-files", With("file", "other.tbl"), callback)
	assert.False(t, triggered, "Should not trigger for mismatching file")

	// 2. Match context
	Inject("fp-files", With("file", "target.tbl"), callback)
	assert.True(t, triggered, "Should trigger for matching file")
}

func TestFailpointComplexConditions(t *testing.T) {
	ClearAll()
	defer ClearAll()

	// Logic: Trigger if file=target.tbl AND count >= 3
	Enable("fp-complex", "file=target.tbl count>=3")

	triggered := false
	callback := func(val string, ctx interface{}) {
		triggered = true
	}

	// 1. Match file, but count=1. Should NOT trigger.
	Inject("fp-complex", With("file", "target.tbl"), callback)
	assert.False(t, triggered, "Count=1 (<3), should not trigger")
	assert.Equal(t, 1, GetCount("fp-complex"))

	// 2. Mismatch file, count=2. Should NOT trigger.
	Inject("fp-complex", With("file", "other.tbl"), callback)
	assert.False(t, triggered, "File mismatch, should not trigger")
	assert.Equal(t, 2, GetCount("fp-complex")) // Count increments even on mismatch?
	// YES, Inject increments counter BEFORE evaluate.

	// 3. Match file, count=3. Should trigger.
	Inject("fp-complex", With("file", "target.tbl"), callback)
	assert.True(t, triggered, "Count=3 (>=3) and file match, should trigger")
	assert.Equal(t, 3, GetCount("fp-complex"))
}
