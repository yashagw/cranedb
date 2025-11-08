package parse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashagw/cranedb/internal/parse/parserdata"
)

func TestParserField(t *testing.T) {
	p := NewParser(NewLexer("MyField"))
	require.NotNil(t, p)

	f, err := p.field()
	require.NoError(t, err)
	assert.Equal(t, "myfield", f)

	// Next token should not be an id; expect error
	_, err = p.field()
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)
}

func TestParserConstant(t *testing.T) {
	// Integer constant
	p1 := NewParser(NewLexer("123"))
	require.NotNil(t, p1)
	val, err := p1.constant()
	require.NoError(t, err)
	assert.Equal(t, 123, val)

	// Single-quoted string
	p2 := NewParser(NewLexer("'hello'"))
	require.NotNil(t, p2)
	val, err = p2.constant()
	require.NoError(t, err)
	assert.Equal(t, "hello", val)

	// Double-quoted string
	p3 := NewParser(NewLexer(`"world"`))
	require.NotNil(t, p3)
	val, err = p3.constant()
	require.NoError(t, err)
	assert.Equal(t, "world", val)

	// Error case
	p4 := NewParser(NewLexer("select"))
	require.NotNil(t, p4)
	_, err = p4.constant()
	assert.Error(t, err)
	assert.Equal(t, ErrBadSyntax, err)
}

func TestParserExpression(t *testing.T) {
	// Field expression
	p1 := NewParser(NewLexer("name"))
	require.NotNil(t, p1)
	e, err := p1.expression()
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.True(t, e.IsFieldName())
	assert.Equal(t, "name", e.AsFieldName())

	// Int constant expression
	p2 := NewParser(NewLexer("42"))
	require.NotNil(t, p2)
	e, err = p2.expression()
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.False(t, e.IsFieldName())
	c := e.AsConstant()
	assert.Equal(t, "42", c.String())

	// String constant expression
	p3 := NewParser(NewLexer("'john'"))
	require.NotNil(t, p3)
	e, err = p3.expression()
	require.NoError(t, err)
	require.NotNil(t, e)
	assert.False(t, e.IsFieldName())
	c = e.AsConstant()
	assert.Equal(t, "john", c.String())
}

func TestParserTerm(t *testing.T) {
	p := NewParser(NewLexer("age = 25"))
	require.NotNil(t, p)
	tm, err := p.term()
	require.NoError(t, err)
	require.NotNil(t, tm)
	assert.Equal(t, "age = 25", tm.String())
}

func TestParserPredicate(t *testing.T) {
	p := NewParser(NewLexer("age = 25 and name = 'John'"))
	require.NotNil(t, p)
	pr, err := p.predicate()
	require.NoError(t, err)
	require.NotNil(t, pr)
	assert.Equal(t, "age = 25 and name = John", pr.String())
}

func TestParserQuery(t *testing.T) {
	t.Run("WithoutWhere", func(t *testing.T) {
		q := "select name, age from students, classes"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		qd, err := p.Query()
		require.NoError(t, err)
		require.NotNil(t, qd)
		assert.Equal(t, []string{"name", "age"}, qd.Fields())
		assert.Equal(t, []string{"students", "classes"}, qd.Tables())
		assert.Nil(t, qd.Predicate())
	})

	t.Run("WithWhere", func(t *testing.T) {
		q := "select name from students where age = 25 and name = 'John'"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		qd, err := p.Query()
		require.NoError(t, err)
		require.NotNil(t, qd)
		assert.Equal(t, []string{"name"}, qd.Fields())
		assert.Equal(t, []string{"students"}, qd.Tables())
		require.NotNil(t, qd.Predicate())
		assert.Equal(t, "age = 25 and name = John", qd.Predicate().String())
	})

	t.Run("CaseInsensitiveKeywords", func(t *testing.T) {
		q := "SELECT Name, Age FROM Students WHERE Age = 30"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		qd, err := p.Query()
		require.NoError(t, err)
		require.NotNil(t, qd)
		assert.Equal(t, []string{"name", "age"}, qd.Fields()) // identifiers lowercased
		assert.Equal(t, []string{"students"}, qd.Tables())
		require.NotNil(t, qd.Predicate())
		assert.Equal(t, "age = 30", qd.Predicate().String())
	})

	t.Run("MissingFromError", func(t *testing.T) {
		q := "select name students"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		_, err := p.Query()
		assert.Error(t, err)
		assert.Equal(t, ErrBadSyntax, err)
	})
}

func TestParserInsert(t *testing.T) {
	t.Run("SimpleInsert", func(t *testing.T) {
		q := "insert into students (name, age) values ('John', 25)"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		cmd, err := p.UpdateCmd()
		require.NoError(t, err)
		require.NotNil(t, cmd)
		ins, ok := cmd.(*parserdata.InsertData)
		require.True(t, ok)
		assert.Equal(t, "students", ins.Table())
		assert.Equal(t, []string{"name", "age"}, ins.Fields())
		assert.Equal(t, []any{"John", 25}, ins.Values())
	})

	t.Run("InsertLowercasesIdentifiersOnly", func(t *testing.T) {
		q := "INSERT INTO Students (Name, Age) VALUES ('Alice', 30)"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		cmd, err := p.UpdateCmd()
		require.NoError(t, err)
		ins := cmd.(*parserdata.InsertData)
		assert.Equal(t, "students", ins.Table())
		assert.Equal(t, []string{"name", "age"}, ins.Fields())
		assert.Equal(t, []any{"Alice", 30}, ins.Values())
	})
}

func TestParserHelpers(t *testing.T) {
	t.Run("fieldList", func(t *testing.T) {
		p := NewParser(NewLexer("Name, Age, Address"))
		require.NotNil(t, p)
		fields, err := p.fieldList()
		require.NoError(t, err)
		assert.Equal(t, []string{"name", "age", "address"}, fields)
	})

	t.Run("tableList", func(t *testing.T) {
		p := NewParser(NewLexer("Students, Classes"))
		require.NotNil(t, p)
		tables, err := p.tableList()
		require.NoError(t, err)
		assert.Equal(t, []string{"students", "classes"}, tables)
	})

	t.Run("constListMixedTypes", func(t *testing.T) {
		p := NewParser(NewLexer(`'John', 25, 'Doe''s'`))
		require.NotNil(t, p)
		vals, err := p.constList()
		require.NoError(t, err)
		assert.Equal(t, []any{"John", 25, "Doe's"}, vals)
	})
}

func TestParserDelete(t *testing.T) {
	t.Run("WithoutWhere", func(t *testing.T) {
		q := "delete from students"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		cmd, err := p.UpdateCmd()
		require.NoError(t, err)
		dd, ok := cmd.(*parserdata.DeleteData)
		require.True(t, ok)
		require.NotNil(t, dd)
		assert.Equal(t, "students", dd.Table())
		assert.Nil(t, dd.Predicate())
	})

	t.Run("WithWhere", func(t *testing.T) {
		q := "delete from students where age = 25 and name = 'John'"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		cmd, err := p.UpdateCmd()
		require.NoError(t, err)
		dd, ok := cmd.(*parserdata.DeleteData)
		require.True(t, ok)
		require.NotNil(t, dd)
		require.NotNil(t, dd.Predicate())
		assert.Equal(t, "age = 25 and name = John", dd.Predicate().String())
	})
}

func TestParserUpdate(t *testing.T) {
	t.Run("WithoutWhere", func(t *testing.T) {
		q := "update students set age = 26"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		cmd, err := p.UpdateCmd()
		require.NoError(t, err)
		ud, ok := cmd.(*parserdata.ModifyData)
		require.True(t, ok)
		require.NotNil(t, ud)
		assert.Equal(t, "students", ud.Table())
		assert.Equal(t, "age", ud.FieldName())
		require.NotNil(t, ud.NewValue())
		assert.False(t, ud.NewValue().IsFieldName())
		constVal := ud.NewValue().AsConstant()
		assert.Equal(t, "26", constVal.String())
	})

	t.Run("WithWhere", func(t *testing.T) {
		q := "update students set name = 'Bob' where age = 25"
		p := NewParser(NewLexer(q))
		require.NotNil(t, p)
		cmd, err := p.UpdateCmd()
		require.NoError(t, err)
		ud, ok := cmd.(*parserdata.ModifyData)
		require.True(t, ok)
		require.NotNil(t, ud)
		assert.Equal(t, "students", ud.Table())
		assert.Equal(t, "name", ud.FieldName())
		require.NotNil(t, ud.NewValue())
		assert.False(t, ud.NewValue().IsFieldName())
		constVal := ud.NewValue().AsConstant()
		assert.Equal(t, "Bob", constVal.String())
		require.NotNil(t, ud.Predicate())
		assert.Equal(t, "age = 25", ud.Predicate().String())
	})
}

func TestParserCreateTable(t *testing.T) {
	t.Run("SimpleCreateTable", func(t *testing.T) {
		stmt := "create table Students ( id int, name varchar(20) )"
		p := NewParser(NewLexer(stmt))
		require.NotNil(t, p)
		cmd, err := p.CreateCmd()
		require.NoError(t, err)
		ct, ok := cmd.(*parserdata.CreateTableData)
		require.True(t, ok)
		require.NotNil(t, ct)
		assert.Equal(t, "students", ct.TableName())
		sch := ct.Schema()
		require.NotNil(t, sch)
		assert.True(t, sch.HasField("id"))
		assert.Equal(t, "int", sch.Type("id"))
		assert.True(t, sch.HasField("name"))
		assert.Equal(t, "string", sch.Type("name"))
		assert.Equal(t, 20, sch.Length("name"))
	})

	t.Run("CaseInsensitiveKeywords", func(t *testing.T) {
		stmt := "CREATE TABLE People ( Age INT, NickName VARCHAR(8) )"
		p := NewParser(NewLexer(stmt))
		require.NotNil(t, p)
		cmd, err := p.CreateCmd()
		require.NoError(t, err)
		ct := cmd.(*parserdata.CreateTableData)
		assert.Equal(t, "people", ct.TableName())
		sch := ct.Schema()
		assert.Equal(t, "int", sch.Type("age"))
		assert.Equal(t, "string", sch.Type("nickname"))
		assert.Equal(t, 8, sch.Length("nickname"))
	})
}

func TestParserCreateView(t *testing.T) {
	stmt := "create view V1 as select name from students where age = 30"
	p := NewParser(NewLexer(stmt))
	require.NotNil(t, p)
	cmd, err := p.CreateCmd()
	require.NoError(t, err)
	cv, ok := cmd.(*parserdata.CreateViewData)
	require.True(t, ok)
	require.NotNil(t, cv)
	assert.Equal(t, "v1", cv.ViewName())
	qd := cv.Query()
	require.NotNil(t, qd)
	assert.Equal(t, []string{"name"}, qd.Fields())
	assert.Equal(t, []string{"students"}, qd.Tables())
	require.NotNil(t, qd.Predicate())
	assert.Equal(t, "age = 30", qd.Predicate().String())
}

func TestParserFieldDefinitionsHelpers(t *testing.T) {
	t.Run("fieldDefsMixed", func(t *testing.T) {
		p := NewParser(NewLexer("id int, name varchar(10), age int"))
		require.NotNil(t, p)
		sch, err := p.fieldDefs()
		require.NoError(t, err)
		require.NotNil(t, sch)
		assert.Equal(t, "int", sch.Type("id"))
		assert.Equal(t, "string", sch.Type("name"))
		assert.Equal(t, 10, sch.Length("name"))
		assert.Equal(t, "int", sch.Type("age"))
	})

	t.Run("fieldDefInt", func(t *testing.T) {
		p := NewParser(NewLexer("age int"))
		require.NotNil(t, p)
		sch, err := p.fieldDef()
		require.NoError(t, err)
		assert.Equal(t, "int", sch.Type("age"))
	})

	t.Run("fieldDefVarchar", func(t *testing.T) {
		p := NewParser(NewLexer("name varchar(12)"))
		require.NotNil(t, p)
		sch, err := p.fieldDef()
		require.NoError(t, err)
		assert.Equal(t, "string", sch.Type("name"))
		assert.Equal(t, 12, sch.Length("name"))
	})
}
