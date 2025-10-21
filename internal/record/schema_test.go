package record

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchema(t *testing.T) {
	// Create new schema
	schema := NewSchema()
	require.NotNil(t, schema)
	assert.Empty(t, schema.fields)

	// Add int field
	schema.AddIntField("id")
	assert.Len(t, schema.fields, 1)
	assert.Equal(t, "id", schema.fields[0])

	fieldInfo, exists := schema.fieldInfo["id"]
	require.True(t, exists)
	assert.Equal(t, "int", fieldInfo.fieldType)
	assert.Equal(t, 4, fieldInfo.fieldLength)

	// Add string field
	schema.AddStringField("name", 50)
	assert.Len(t, schema.fields, 2)
	assert.Equal(t, "name", schema.fields[1])

	fieldInfo, exists = schema.fieldInfo["name"]
	require.True(t, exists)
	assert.Equal(t, "string", fieldInfo.fieldType)
	assert.Equal(t, 50, fieldInfo.fieldLength)

	// Test copy all fields
	targetSchema := NewSchema()
	targetSchema.CopyAll(schema)

	assert.Len(t, targetSchema.fields, 2)
	assert.Equal(t, "id", targetSchema.fields[0])
	assert.Equal(t, "name", targetSchema.fields[1])

	// Verify both fields were copied correctly
	idInfo, exists := targetSchema.fieldInfo["id"]
	require.True(t, exists)
	assert.Equal(t, "int", idInfo.fieldType)
	assert.Equal(t, 4, idInfo.fieldLength)

	nameInfo, exists := targetSchema.fieldInfo["name"]
	require.True(t, exists)
	assert.Equal(t, "string", nameInfo.fieldType)
	assert.Equal(t, 50, nameInfo.fieldLength)
}
