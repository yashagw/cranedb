package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLayout(t *testing.T) {
	// Create a schema for testing
	schema := NewSchema()
	schema.AddIntField("id")
	schema.AddBoolField("active")
	schema.AddStringField("name", 20)

	// Create layout from schema
	layout := NewLayoutFromSchema(schema)
	require.NotNil(t, layout)
	assert.Equal(t, schema, layout.schema)

	// Check slot size calculation
	// 4 bytes (empty/inuse flag) + 4 bytes (id) + 1 bytes (active) + 20 bytes (name) + 4 (name string len) = 33 bytes
	expectedSlotSize := 4 + 4 + 1 + 20 + 4
	assert.Equal(t, expectedSlotSize, layout.GetSlotSize())

	// Check field offsets
	assert.Equal(t, 4, layout.GetOffset("id"))     // After empty/inuse flag
	assert.Equal(t, 8, layout.GetOffset("active")) // After id (4 bytes)
	assert.Equal(t, 9, layout.GetOffset("name"))   // After active (1 bytes)

	// Check offset for non-existent field
	assert.Equal(t, 0, layout.GetOffset("nonexistent"))
}
