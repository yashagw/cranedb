package parse

import (
	"github.com/yashagw/cranedb/internal/parse/parserdata"
	"github.com/yashagw/cranedb/internal/query"
	"github.com/yashagw/cranedb/internal/record"
)

// Parser is a parser for the Cranedb query language.
type Parser struct {
	lexer *Lexer
}

// NewParser creates a new Parser.
func NewParser(lexer *Lexer) *Parser {
	return &Parser{
		lexer: lexer,
	}
}

// NewParserFromString creates a new Parser from a string.
func NewParserFromString(sql string) *Parser {
	lexer := NewLexer(sql)
	return NewParser(lexer)
}

func (p *Parser) field() (string, error) {
	id, err := p.lexer.EatId()
	if err != nil {
		return "", err
	}
	return id, nil
}

func (p *Parser) constant() (any, error) {
	if p.lexer.MatchIntConstant() {
		val, err := p.lexer.EatIntConstant()
		if err != nil {
			return 0, err
		}
		return val, nil
	}
	if p.lexer.MatchStringConstant() {
		val, err := p.lexer.EatStringConstant()
		if err != nil {
			return "", err
		}
		return val, nil
	}
	return nil, ErrBadSyntax
}

func (p *Parser) expression() (*query.Expression, error) {
	if p.lexer.MatchId() {
		id, err := p.field()
		if err != nil {
			return nil, err
		}
		return query.NewFieldNameExpression(id), nil
	}
	if p.lexer.MatchIntConstant() || p.lexer.MatchStringConstant() {
		val, err := p.constant()
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case int:
			return query.NewConstantExpression(*query.NewIntConstant(v)), nil
		case string:
			return query.NewConstantExpression(*query.NewStringConstant(v)), nil
		default:
			return nil, ErrBadSyntax
		}
	}
	return nil, ErrBadSyntax
}

func (p *Parser) term() (*query.Term, error) {
	left, err := p.expression()
	if err != nil {
		return nil, err
	}
	err = p.lexer.EatDelim('=')
	if err != nil {
		return nil, err
	}
	right, err := p.expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(*left, *right), nil
}

func (p *Parser) predicate() (*query.Predicate, error) {
	firstTerm, err := p.term()
	if err != nil {
		return nil, err
	}
	pred := query.NewPredicate(*firstTerm)
	for p.lexer.MatchKeyword("and") {
		p.lexer.EatKeyword("and")
		term, err := p.term()
		if err != nil {
			return nil, err
		}
		pred.ConjunctWith(*query.NewPredicate(*term))
	}
	return pred, nil
}

func (p *Parser) Query() (*parserdata.QueryData, error) {
	// Select
	err := p.lexer.EatKeyword("select")
	if err != nil {
		return nil, err
	}
	// Field List
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	// From
	err = p.lexer.EatKeyword("from")
	if err != nil {
		return nil, err
	}
	// Table List
	tableNames, err := p.tableList()
	if err != nil {
		return nil, err
	}

	if !p.lexer.MatchKeyword("where") {
		return parserdata.NewQueryData(fields, tableNames, nil), nil
	}

	// Where
	err = p.lexer.EatKeyword("where")
	if err != nil {
		return nil, err
	}

	// Predicate
	predicate, err := p.predicate()
	if err != nil {
		return nil, err
	}

	return parserdata.NewQueryData(fields, tableNames, predicate), nil
}

func (p *Parser) UpdateCmd() (interface{}, error) {
	if p.lexer.MatchKeyword("insert") {
		return p.insert()
	}
	if p.lexer.MatchKeyword("update") {
		return p.modify()
	}
	if p.lexer.MatchKeyword("delete") {
		return p.delete()
	}
	return p.CreateCmd()
}

func (p *Parser) CreateCmd() (interface{}, error) {
	err := p.lexer.EatKeyword("create")
	if err != nil {
		return nil, err
	}

	if p.lexer.MatchKeyword("table") {
		return p.createTable()
	} else if p.lexer.MatchKeyword("view") {
		return p.createView()
	} else {
		return nil, ErrBadSyntax
	}
}

func (p *Parser) createTable() (*parserdata.CreateTableData, error) {
	// Create is already eaten by CreateCmd()

	// Table Name
	err := p.lexer.EatKeyword("table")
	if err != nil {
		return nil, err
	}
	// Table Name
	tableName, err := p.field()
	if err != nil {
		return nil, err
	}
	// (
	err = p.lexer.EatDelim('(')
	if err != nil {
		return nil, err
	}
	// Field Definitions
	schema, err := p.fieldDefs()
	if err != nil {
		return nil, err
	}
	// )
	err = p.lexer.EatDelim(')')
	if err != nil {
		return nil, err
	}

	return parserdata.NewCreateTableData(tableName, schema), nil
}

func (p *Parser) createView() (*parserdata.CreateViewData, error) {
	// Create is already eaten by CreateCmd()

	// View Name
	err := p.lexer.EatKeyword("view")
	if err != nil {
		return nil, err
	}
	// View Name
	viewName, err := p.field()
	if err != nil {
		return nil, err
	}
	// As
	err = p.lexer.EatKeyword("as")
	if err != nil {
		return nil, err
	}
	// Query
	query, err := p.Query()
	if err != nil {
		return nil, err
	}
	return parserdata.NewCreateViewData(viewName, query), nil
}

func (p *Parser) insert() (*parserdata.InsertData, error) {
	// Insert
	err := p.lexer.EatKeyword("insert")
	if err != nil {
		return nil, err
	}
	// Into
	err = p.lexer.EatKeyword("into")
	if err != nil {
		return nil, err
	}
	// Table
	table, err := p.field()
	if err != nil {
		return nil, err
	}
	// (
	err = p.lexer.EatDelim('(')
	if err != nil {
		return nil, err
	}
	// Fields
	fields, err := p.fieldList()
	if err != nil {
		return nil, err
	}
	// )
	err = p.lexer.EatDelim(')')
	if err != nil {
		return nil, err
	}
	// Values
	err = p.lexer.EatKeyword("values")
	if err != nil {
		return nil, err
	}
	// (
	err = p.lexer.EatDelim('(')
	if err != nil {
		return nil, err
	}
	// Values
	values, err := p.constList()
	if err != nil {
		return nil, err
	}
	// )
	err = p.lexer.EatDelim(')')
	if err != nil {
		return nil, err
	}

	return parserdata.NewInsertData(table, fields, values), nil
}

func (p *Parser) delete() (*parserdata.DeleteData, error) {
	// Delete
	err := p.lexer.EatKeyword("delete")
	if err != nil {
		return nil, err
	}
	// From
	err = p.lexer.EatKeyword("from")
	if err != nil {
		return nil, err
	}
	// Table
	table, err := p.field()
	if err != nil {
		return nil, err
	}

	if !p.lexer.MatchKeyword("where") {
		return parserdata.NewDeleteData(table, nil), nil
	}

	// Where
	if err := p.lexer.EatKeyword("where"); err != nil {
		return nil, err
	}
	predicate, err := p.predicate()
	if err != nil {
		return nil, err
	}

	return parserdata.NewDeleteData(table, predicate), nil
}

func (p *Parser) modify() (*parserdata.ModifyData, error) {
	// Update
	err := p.lexer.EatKeyword("update")
	if err != nil {
		return nil, err
	}
	// Table
	table, err := p.field()
	if err != nil {
		return nil, err
	}
	// Set
	err = p.lexer.EatKeyword("set")
	if err != nil {
		return nil, err
	}
	// Field
	field, err := p.field()
	if err != nil {
		return nil, err
	}
	// =
	err = p.lexer.EatDelim('=')
	if err != nil {
		return nil, err
	}
	// Value
	value, err := p.expression()
	if err != nil {
		return nil, err
	}

	if !p.lexer.MatchKeyword("where") {
		return parserdata.NewModifyData(table, field, value, nil), nil
	}

	// Where
	if err := p.lexer.EatKeyword("where"); err != nil {
		return nil, err
	}
	predicate, err := p.predicate()
	if err != nil {
		return nil, err
	}

	return parserdata.NewModifyData(table, field, value, predicate), nil
}

func (p *Parser) fieldList() ([]string, error) {
	fields := []string{}

	firstField, err := p.field()
	if err != nil {
		return nil, err
	}
	fields = append(fields, firstField)

	// Now look for ", field" patterns.
	for p.lexer.MatchDelim(',') {
		err = p.lexer.EatDelim(',')
		if err != nil {
			return nil, err
		}
		field, err := p.field()
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}

	return fields, nil
}

func (p *Parser) tableList() ([]string, error) {
	tableNames := []string{}

	firstTable, err := p.lexer.EatId()
	if err != nil {
		return nil, err
	}
	tableNames = append(tableNames, firstTable)

	// Now look for ", table" patterns.
	for p.lexer.MatchDelim(',') {
		err = p.lexer.EatDelim(',')
		if err != nil {
			return nil, err
		}
		table, err := p.lexer.EatId()
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, table)
	}

	return tableNames, nil
}

func (p *Parser) constList() ([]any, error) {
	consts := []any{}

	firstConst, err := p.constant()
	if err != nil {
		return nil, err
	}
	consts = append(consts, firstConst)

	// Now look for ", const" patterns.
	for p.lexer.MatchDelim(',') {
		err = p.lexer.EatDelim(',')
		if err != nil {
			return nil, err
		}
		nextConst, err := p.constant()
		if err != nil {
			return nil, err
		}
		consts = append(consts, nextConst)
	}

	return consts, nil
}

func (p *Parser) fieldDefs() (*record.Schema, error) {
	firstFieldDef, err := p.fieldDef()
	if err != nil {
		return nil, err
	}

	for p.lexer.MatchDelim(',') {
		err = p.lexer.EatDelim(',')
		if err != nil {
			return nil, err
		}
		nextFieldDef, err := p.fieldDef()
		if err != nil {
			return nil, err
		}
		firstFieldDef.CopyAll(nextFieldDef)
	}

	return firstFieldDef, nil
}

func (p *Parser) fieldDef() (*record.Schema, error) {
	fieldName, err := p.field()
	if err != nil {
		return nil, err
	}
	return p.fieldType(fieldName)
}

func (p *Parser) fieldType(fieldName string) (*record.Schema, error) {
	schema := record.NewSchema()

	if p.lexer.MatchKeyword("int") {
		err := p.lexer.EatKeyword("int")
		if err != nil {
			return nil, err
		}
		schema.AddIntField(fieldName)
		return schema, nil
	} else if p.lexer.MatchKeyword("varchar") {
		err := p.lexer.EatKeyword("varchar")
		if err != nil {
			return nil, err
		}
		err = p.lexer.EatDelim('(')
		if err != nil {
			return nil, err
		}
		length, err := p.lexer.EatIntConstant()
		if err != nil {
			return nil, err
		}
		err = p.lexer.EatDelim(')')
		if err != nil {
			return nil, err
		}
		schema.AddStringField(fieldName, length)
		return schema, nil
	} else {
		return nil, ErrBadSyntax
	}
}
