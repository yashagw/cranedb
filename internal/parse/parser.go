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
	if p.lexer.MatchBoolConstant() {
		val, err := p.lexer.EatBoolConstant()
		if err != nil {
			return false, err
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
	if p.lexer.MatchIntConstant() || p.lexer.MatchStringConstant() || p.lexer.MatchBoolConstant() {
		val, err := p.constant()
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case int:
			return query.NewConstantExpression(*query.NewIntConstant(v)), nil
		case string:
			return query.NewConstantExpression(*query.NewStringConstant(v)), nil
		case bool:
			return query.NewConstantExpression(*query.NewBoolConstant(v)), nil
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

	var operator query.Operator
	if p.lexer.MatchDelim('>') {
		err = p.lexer.EatDelim('>')
		if err != nil {
			return nil, err
		}
		if p.lexer.MatchDelim('=') {
			err = p.lexer.EatDelim('=')
			if err != nil {
				return nil, err
			}
			operator = query.OpGE
		} else {
			operator = query.OpGT
		}
	} else if p.lexer.MatchDelim('<') {
		err = p.lexer.EatDelim('<')
		if err != nil {
			return nil, err
		}
		if p.lexer.MatchDelim('=') {
			err = p.lexer.EatDelim('=')
			if err != nil {
				return nil, err
			}
			operator = query.OpLE
		} else {
			operator = query.OpLT
		}
	} else if p.lexer.MatchDelim('!') {
		err = p.lexer.EatDelim('!')
		if err != nil {
			return nil, err
		}
		err = p.lexer.EatDelim('=')
		if err != nil {
			return nil, err
		}
		operator = query.OpNE
	} else {
		err = p.lexer.EatDelim('=')
		if err != nil {
			return nil, err
		}
		operator = query.OpEQ
	}

	right, err := p.expression()
	if err != nil {
		return nil, err
	}
	return query.NewTerm(*left, *right, operator), nil
}

// predicate parses a full boolean predicate expression supporting AND, OR, and parentheses.
// Precedence: AND binds tighter than OR
func (p *Parser) predicate() (*query.Predicate, error) {
	return p.parseOrExpr()
}

// parseOrExpr parses left-associative OR expressions: AndExpr ('or' AndExpr)*
func (p *Parser) parseOrExpr() (*query.Predicate, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.lexer.MatchKeyword("or") {
		if err := p.lexer.EatKeyword("or"); err != nil {
			return nil, err
		}
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = query.Or(left, right)
	}

	return left, nil
}

// parseAndExpr parses left-associative AND expressions: PrimaryPred ('and' PrimaryPred)*
func (p *Parser) parseAndExpr() (*query.Predicate, error) {
	left, err := p.parsePrimaryPred()
	if err != nil {
		return nil, err
	}

	for p.lexer.MatchKeyword("and") {
		if err := p.lexer.EatKeyword("and"); err != nil {
			return nil, err
		}
		right, err := p.parsePrimaryPred()
		if err != nil {
			return nil, err
		}
		left = query.And(left, right)
	}

	return left, nil
}

// parsePrimaryPred parses either a single comparison term or a parenthesized predicate.
func (p *Parser) parsePrimaryPred() (*query.Predicate, error) {
	if p.lexer.MatchDelim('(') {
		if err := p.lexer.EatDelim('('); err != nil {
			return nil, err
		}
		sub, err := p.predicate()
		if err != nil {
			return nil, err
		}
		if err := p.lexer.EatDelim(')'); err != nil {
			return nil, err
		}
		return sub, nil
	}

	t, err := p.term()
	if err != nil {
		return nil, err
	}
	return query.NewPredicate(*t), nil
}

func (p *Parser) Query() (*parserdata.QueryData, error) {
	// Select
	err := p.lexer.EatKeyword("select")
	if err != nil {
		return nil, err
	}
	// Field List (may include aggregation functions)
	fields, aggregationFns, err := p.selectFieldList()
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

	var predicate *query.Predicate
	var sortFields []string
	var groupFields []string

	// Parse WHERE clause if present
	if p.lexer.MatchKeyword("where") {
		err = p.lexer.EatKeyword("where")
		if err != nil {
			return nil, err
		}
		predicate, err = p.predicate()
		if err != nil {
			return nil, err
		}
	}

	// Parse GROUP BY clause if present
	if p.lexer.MatchKeyword("group") {
		err = p.lexer.EatKeyword("group")
		if err != nil {
			return nil, err
		}
		err = p.lexer.EatKeyword("by")
		if err != nil {
			return nil, err
		}
		groupFields, err = p.sortFieldList()
		if err != nil {
			return nil, err
		}
	}

	// Parse ORDER BY clause if present
	if p.lexer.MatchKeyword("order") {
		err = p.lexer.EatKeyword("order")
		if err != nil {
			return nil, err
		}
		err = p.lexer.EatKeyword("by")
		if err != nil {
			return nil, err
		}
		sortFields, err = p.sortFieldList()
		if err != nil {
			return nil, err
		}
	}

	// Parse LIMIT clause if present
	var limit int
	if p.lexer.MatchKeyword("limit") {
		err = p.lexer.EatKeyword("limit")
		if err != nil {
			return nil, err
		}
		limit, err = p.lexer.EatIntConstant()
		if err != nil {
			return nil, err
		}
	}

	// Parse OFFSET clause if present
	var offset int
	if p.lexer.MatchKeyword("offset") {
		err = p.lexer.EatKeyword("offset")
		if err != nil {
			return nil, err
		}
		offset, err = p.lexer.EatIntConstant()
		if err != nil {
			return nil, err
		}
	}

	qd := parserdata.NewQueryDataWithGroupBy(fields, tableNames, predicate, sortFields, groupFields, aggregationFns)
	qd.SetLimit(limit)
	qd.SetOffset(offset)
	return qd, nil
}

// Explain parses EXPLAIN <query> and returns the query data wrapped in ExplainData
func (p *Parser) Explain() (*parserdata.ExplainData, error) {
	err := p.lexer.EatKeyword("explain")
	if err != nil {
		return nil, err
	}
	queryData, err := p.Query()
	if err != nil {
		return nil, err
	}
	return parserdata.NewExplainData(queryData), nil
}

func (p *Parser) UpdateCmd() (interface{}, error) {
	if p.lexer.MatchKeyword("begin") {
		return p.begin()
	}
	if p.lexer.MatchKeyword("commit") {
		return p.commit()
	}
	if p.lexer.MatchKeyword("rollback") {
		return p.rollback()
	}
	if p.lexer.MatchKeyword("insert") {
		return p.insert()
	}
	if p.lexer.MatchKeyword("update") {
		return p.modify()
	}
	if p.lexer.MatchKeyword("delete") {
		return p.delete()
	}
	if p.lexer.MatchKeyword("set") {
		return p.set()
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
	} else if p.lexer.MatchKeyword("index") {
		return p.createIndex()
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

func (p *Parser) createIndex() (*parserdata.CreateIndexData, error) {
	// Create is already eaten by CreateCmd()

	// Index Keyword
	err := p.lexer.EatKeyword("index")
	if err != nil {
		return nil, err
	}

	// Index Name
	indexName, err := p.field()
	if err != nil {
		return nil, err
	}

	// On Keyword
	err = p.lexer.EatKeyword("on")
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

	// Field Name
	fieldName, err := p.field()
	if err != nil {
		return nil, err
	}

	// )
	err = p.lexer.EatDelim(')')
	if err != nil {
		return nil, err
	}
	return parserdata.NewCreateIndexData(indexName, tableName, fieldName), nil
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

func (p *Parser) set() (*parserdata.SetData, error) {
	// Set
	err := p.lexer.EatKeyword("set")
	if err != nil {
		return nil, err
	}
	// Variable name
	variableName, err := p.field()
	if err != nil {
		return nil, err
	}
	// =
	err = p.lexer.EatDelim('=')
	if err != nil {
		return nil, err
	}
	// Value - can be boolean (true/false), string, or int
	var value interface{}
	if p.lexer.MatchKeyword("true") {
		p.lexer.EatKeyword("true")
		value = true
	} else if p.lexer.MatchKeyword("false") {
		p.lexer.EatKeyword("false")
		value = false
	} else if p.lexer.MatchStringConstant() {
		val, err := p.lexer.EatStringConstant()
		if err != nil {
			return nil, err
		}
		value = val
	} else if p.lexer.MatchIntConstant() {
		val, err := p.lexer.EatIntConstant()
		if err != nil {
			return nil, err
		}
		value = val
	} else {
		return nil, ErrBadSyntax
	}

	return parserdata.NewSetData(variableName, value), nil
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

// selectFieldList parses a SELECT field list that may include aggregation functions.
// Returns: (regular fields, aggregation functions, error)
func (p *Parser) selectFieldList() ([]string, []parserdata.AggregationFn, error) {
	fields := []string{}
	aggFns := []parserdata.AggregationFn{}

	// Parse first field (may be aggregation or regular field)
	field, aggFn, err := p.selectField()
	if err != nil {
		return nil, nil, err
	}
	if aggFn != nil {
		aggFns = append(aggFns, *aggFn)
	} else {
		fields = append(fields, field)
	}

	// Now look for ", field" patterns.
	for p.lexer.MatchDelim(',') {
		err = p.lexer.EatDelim(',')
		if err != nil {
			return nil, nil, err
		}
		field, aggFn, err := p.selectField()
		if err != nil {
			return nil, nil, err
		}
		if aggFn != nil {
			aggFns = append(aggFns, *aggFn)
		} else {
			fields = append(fields, field)
		}
	}

	return fields, aggFns, nil
}

// selectField parses a single SELECT field, which may be:
// - A regular field: fieldname
// - An aggregation function: max(fieldname), min(fieldname), count(fieldname), sum(fieldname), or distinct(fieldname)
// Returns: (field name or empty if aggregation, aggregation function or nil, error)
func (p *Parser) selectField() (string, *parserdata.AggregationFn, error) {
	// Check if it's an aggregation function: max(...) or min(...)
	if p.lexer.MatchKeyword("max") {
		err := p.lexer.EatKeyword("max")
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim('(')
		if err != nil {
			return "", nil, err
		}
		fieldName, err := p.field()
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim(')')
		if err != nil {
			return "", nil, err
		}
		aggFn := parserdata.AggregationFn{
			Type:      parserdata.AggMax,
			FieldName: fieldName,
		}
		return "", &aggFn, nil
	}

	if p.lexer.MatchKeyword("distinct") {
		err := p.lexer.EatKeyword("distinct")
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim('(')
		if err != nil {
			return "", nil, err
		}
		fieldName, err := p.field()
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim(')')
		if err != nil {
			return "", nil, err
		}
		aggFn := parserdata.AggregationFn{
			Type:      parserdata.AggDistinct,
			FieldName: fieldName,
		}
		return "", &aggFn, nil
	}

	if p.lexer.MatchKeyword("min") {
		err := p.lexer.EatKeyword("min")
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim('(')
		if err != nil {
			return "", nil, err
		}
		fieldName, err := p.field()
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim(')')
		if err != nil {
			return "", nil, err
		}
		aggFn := parserdata.AggregationFn{
			Type:      parserdata.AggMin,
			FieldName: fieldName,
		}
		return "", &aggFn, nil
	}

	if p.lexer.MatchKeyword("count") {
		err := p.lexer.EatKeyword("count")
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim('(')
		if err != nil {
			return "", nil, err
		}
		fieldName, err := p.field()
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim(')')
		if err != nil {
			return "", nil, err
		}
		aggFn := parserdata.AggregationFn{
			Type:      parserdata.AggCount,
			FieldName: fieldName,
		}
		return "", &aggFn, nil
	}

	if p.lexer.MatchKeyword("sum") {
		err := p.lexer.EatKeyword("sum")
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim('(')
		if err != nil {
			return "", nil, err
		}
		fieldName, err := p.field()
		if err != nil {
			return "", nil, err
		}
		err = p.lexer.EatDelim(')')
		if err != nil {
			return "", nil, err
		}
		aggFn := parserdata.AggregationFn{
			Type:      parserdata.AggSum,
			FieldName: fieldName,
		}
		return "", &aggFn, nil
	}

	// Regular field
	field, err := p.field()
	if err != nil {
		return "", nil, err
	}
	return field, nil, nil
}

func (p *Parser) sortFieldList() ([]string, error) {
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
	} else if p.lexer.MatchKeyword("bool") {
		err := p.lexer.EatKeyword("bool")
		if err != nil {
			return nil, err
		}
		schema.AddBoolField(fieldName)
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

func (p *Parser) begin() (*parserdata.BeginData, error) {
	err := p.lexer.EatKeyword("begin")
	if err != nil {
		return nil, err
	}
	// Optionally consume "transaction" keyword if present
	if p.lexer.MatchKeyword("transaction") {
		p.lexer.EatKeyword("transaction")
	}
	return parserdata.NewBeginData(), nil
}

func (p *Parser) commit() (*parserdata.CommitData, error) {
	err := p.lexer.EatKeyword("commit")
	if err != nil {
		return nil, err
	}
	return parserdata.NewCommitData(), nil
}

func (p *Parser) rollback() (*parserdata.RollbackData, error) {
	err := p.lexer.EatKeyword("rollback")
	if err != nil {
		return nil, err
	}
	return parserdata.NewRollbackData(), nil
}
