package query

// createEqualsPredicate creates a predicate that checks if a field equals a value
func createEqualsPredicate(fieldName string, value interface{}) *Predicate {
	fieldExpr := NewFieldNameExpression(fieldName)
	var constExpr *Expression
	switch v := value.(type) {
	case int:
		constExpr = NewConstantExpression(*NewIntConstant(v))
	case string:
		constExpr = NewConstantExpression(*NewStringConstant(v))
	default:
		panic("unsupported value type")
	}
	term := NewTerm(*fieldExpr, *constExpr)
	return NewPredicate(*term)
}

// createCompoundPredicate creates a predicate that checks multiple conditions (AND)
func createCompoundPredicate(conditions []struct {
	fieldName string
	value     interface{}
}) *Predicate {
	if len(conditions) == 0 {
		panic("no conditions provided")
	}

	// Create first predicate
	predicate := createEqualsPredicate(conditions[0].fieldName, conditions[0].value)

	// Add remaining conditions with AND
	for i := 1; i < len(conditions); i++ {
		cond := conditions[i]
		nextPredicate := createEqualsPredicate(cond.fieldName, cond.value)
		predicate.ConjunctWith(*nextPredicate)
	}

	return predicate
}
