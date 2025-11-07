package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLexerEatDelim(t *testing.T) {
	lexer := NewLexer("( )")
	require.NotNil(t, lexer)

	err := lexer.EatDelim('(')
	require.NoError(t, err)

	err = lexer.EatDelim(')')
	require.NoError(t, err)

	// Test error case
	lexer2 := NewLexer("(")
	require.NotNil(t, lexer2)

	err = lexer2.EatDelim(')')
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)
}

func TestLexerEatIntConstant(t *testing.T) {
	lexer := NewLexer("123 456")
	require.NotNil(t, lexer)

	val, err := lexer.EatIntConstant()
	require.NoError(t, err)
	assert.Equal(t, 123, val)

	val, err = lexer.EatIntConstant()
	require.NoError(t, err)
	assert.Equal(t, 456, val)

	// Test error case
	lexer2 := NewLexer("abc")
	require.NotNil(t, lexer2)

	_, err = lexer2.EatIntConstant()
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)
}

func TestLexerEatStringConstant(t *testing.T) {
	// Test single-quoted strings
	lexer := NewLexer("'hello' 'world'")
	require.NotNil(t, lexer)

	val, err := lexer.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	val, err = lexer.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "world", val)

	// Test double-quoted strings
	lexer2 := NewLexer(`"test"`)
	require.NotNil(t, lexer2)

	val, err = lexer2.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "test", val)

	// Test escaped quote in single-quoted string
	lexer3 := NewLexer("'John''s name'")
	require.NotNil(t, lexer3)

	val, err = lexer3.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "John's name", val)

	// Test error case
	lexer4 := NewLexer("abc")
	require.NotNil(t, lexer4)

	_, err = lexer4.EatStringConstant()
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)
}

func TestLexerEatKeyword(t *testing.T) {
	lexer := NewLexer("select from where")
	require.NotNil(t, lexer)

	err := lexer.EatKeyword("select")
	require.NoError(t, err)

	err = lexer.EatKeyword("from")
	require.NoError(t, err)

	err = lexer.EatKeyword("where")
	require.NoError(t, err)

	// Test case insensitivity
	lexer2 := NewLexer("SELECT FROM")
	require.NotNil(t, lexer2)

	err = lexer2.EatKeyword("select")
	require.NoError(t, err)

	err = lexer2.EatKeyword("from")
	require.NoError(t, err)

	// Test error case
	lexer3 := NewLexer("select")
	require.NotNil(t, lexer3)

	err = lexer3.EatKeyword("from")
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)
}

func TestLexerEatId(t *testing.T) {
	lexer := NewLexer("mytable myfield")
	require.NotNil(t, lexer)

	id, err := lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "mytable", id)

	id, err = lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "myfield", id)

	// Test that keywords are not eaten as IDs
	lexer2 := NewLexer("select")
	require.NotNil(t, lexer2)

	_, err = lexer2.EatId()
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)

	// Test lowercase conversion
	lexer3 := NewLexer("MyTable")
	require.NotNil(t, lexer3)

	id, err = lexer3.EatId()
	require.NoError(t, err)
	assert.Equal(t, "mytable", id)
}

func TestLexerComplexQuery(t *testing.T) {
	query := "select name, age from students where age = 25 and name = 'John'"
	lexer := NewLexer(query)
	require.NotNil(t, lexer)

	err := lexer.EatKeyword("select")
	require.NoError(t, err)

	id, err := lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "name", id)

	err = lexer.EatDelim(',')
	require.NoError(t, err)

	id, err = lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "age", id)

	err = lexer.EatKeyword("from")
	require.NoError(t, err)

	id, err = lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "students", id)

	err = lexer.EatKeyword("where")
	require.NoError(t, err)

	id, err = lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "age", id)

	err = lexer.EatDelim('=')
	require.NoError(t, err)

	val, err := lexer.EatIntConstant()
	require.NoError(t, err)
	assert.Equal(t, 25, val)

	err = lexer.EatKeyword("and")
	require.NoError(t, err)

	id, err = lexer.EatId()
	require.NoError(t, err)
	assert.Equal(t, "name", id)

	err = lexer.EatDelim('=')
	require.NoError(t, err)

	str, err := lexer.EatStringConstant()
	require.NoError(t, err)
	assert.Equal(t, "John", str)
}
