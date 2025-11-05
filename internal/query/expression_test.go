package query

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/buffer"
	"github.com/yashagw/cranedb/internal/file"
	"github.com/yashagw/cranedb/internal/log"
	"github.com/yashagw/cranedb/internal/record"
	"github.com/yashagw/cranedb/internal/transaction"
)

func TestExpressionBasic(t *testing.T) {
	// Test creating constant expression
	intConst := NewIntConstant(42)
	constExpr := NewConstantExpression(*intConst)
	require.NotNil(t, constExpr)
	assert.False(t, constExpr.IsFieldName())
	assert.Equal(t, *intConst, constExpr.AsConstant())
	assert.Equal(t, "42", constExpr.String())

	// Test creating string constant expression
	strConst := NewStringConstant("hello")
	strExpr := NewConstantExpression(*strConst)
	require.NotNil(t, strExpr)
	assert.False(t, strExpr.IsFieldName())
	assert.Equal(t, *strConst, strExpr.AsConstant())
	assert.Equal(t, "hello", strExpr.String())

	// Test creating field name expression
	fieldExpr := NewFieldNameExpression("age")
	require.NotNil(t, fieldExpr)
	assert.True(t, fieldExpr.IsFieldName())
	assert.Equal(t, "age", fieldExpr.AsFieldName())
	assert.Equal(t, "age", fieldExpr.String())

	// Test AppliesTo with constant expression (should always return true)
	schema := record.NewSchema()
	schema.AddIntField("age")
	assert.True(t, constExpr.AppliesTo(schema))
	assert.True(t, strExpr.AppliesTo(schema))

	// Test AppliesTo with field name expression
	assert.True(t, fieldExpr.AppliesTo(schema)) // field exists in schema

	fieldExprMissing := NewFieldNameExpression("missing")
	assert.False(t, fieldExprMissing.AppliesTo(schema)) // field doesn't exist

	// Test Evaluate with constant expression (doesn't need scan)
	evaluatedConst := constExpr.Evaluate(nil)
	assert.Equal(t, *intConst, evaluatedConst)
}

// constantScanWrapper wraps a TableScan to return Constants from GetValue
type constantScanWrapper struct {
	*record.TableScan
	schema *record.Schema
}

func (w *constantScanWrapper) GetValue(fldname string) any {
	fieldType := w.schema.Type(fldname)
	if fieldType == "int" {
		return *NewIntConstant(w.TableScan.GetInt(fldname))
	}
	return *NewStringConstant(w.TableScan.GetString(fldname))
}

func TestExpressionEvaluate(t *testing.T) {
	testDir := "/tmp/testdb_expression"
	defer os.RemoveAll(testDir)

	fileManager, err := file.NewManager(testDir, 400)
	require.NoError(t, err)
	logManager, err := log.NewManager(fileManager, "test.log")
	require.NoError(t, err)
	bufferManager, err := buffer.NewManager(fileManager, logManager, 10)
	require.NoError(t, err)
	lockTable := transaction.NewLockTable()

	tx := transaction.NewTransaction(fileManager, logManager, bufferManager, lockTable)
	require.NotNil(t, tx)

	// Create schema with int and string fields
	schema := record.NewSchema()
	schema.AddIntField("age")
	schema.AddStringField("name", 20)

	layout := record.NewLayoutFromSchema(schema)
	require.NotNil(t, layout)

	// Create TableScan
	ts := record.NewTableScan(tx, layout, "TestTable")
	require.NotNil(t, ts)

	// Wrap TableScan to return Constants
	wrappedScan := &constantScanWrapper{
		TableScan: ts,
		schema:    schema,
	}

	// Insert a record
	ts.Insert()
	ts.SetInt("age", 25)
	ts.SetString("name", "John")

	// Test Evaluate with field name expression for int field
	fieldExprAge := NewFieldNameExpression("age")
	evaluatedAge := fieldExprAge.Evaluate(wrappedScan)
	assert.Equal(t, 25, evaluatedAge.AsInt())

	// Test Evaluate with field name expression for string field
	fieldExprName := NewFieldNameExpression("name")
	evaluatedName := fieldExprName.Evaluate(wrappedScan)
	assert.Equal(t, "John", evaluatedName.AsString())

	// Test Evaluate with constant expression (doesn't use scan)
	constExpr := NewConstantExpression(*NewIntConstant(100))
	evaluatedConst := constExpr.Evaluate(wrappedScan)
	assert.Equal(t, 100, evaluatedConst.AsInt())

	// Cleanup
	ts.Close()
	tx.Commit()
}
