package query

import "fmt"

// Constant represents either an integer or string constant value.
type Constant struct {
	intVal *int
	strVal *string
}

// NewIntConstant creates a new Constant with an integer value.
func NewIntConstant(val int) *Constant {
	return &Constant{
		intVal: &val,
	}
}

// NewStringConstant creates a new Constant with a string value.
func NewStringConstant(val string) *Constant {
	return &Constant{
		strVal: &val,
	}
}

// String returns a string representation of the constant.
func (c *Constant) String() string {
	if c.intVal != nil {
		return fmt.Sprintf("%d", *c.intVal)
	}
	return *c.strVal
}

// asInt returns the integer value of the constant.
func (c *Constant) AsInt() int {
	return *c.intVal
}

// asString returns the string value of the constant.
func (c *Constant) AsString() string {
	return *c.strVal
}

// equals checks if the constant is equal to another constant.
func (c *Constant) Equals(other *Constant) bool {
	if c.intVal != nil && other.intVal != nil {
		return *c.intVal == *other.intVal
	}
	if c.strVal != nil && other.strVal != nil {
		return *c.strVal == *other.strVal
	}
	return false
}

// compareTo returns -1, 0, or 1 if this Constant is less than, equal to, or greater than the other, respectively.
// Returns -1 if types do not match.
func (c *Constant) CompareTo(other *Constant) int {
	if c.intVal != nil && other.intVal != nil {
		if *c.intVal < *other.intVal {
			return -1
		} else if *c.intVal > *other.intVal {
			return 1
		} else {
			return 0
		}
	}
	if c.strVal != nil && other.strVal != nil {
		if *c.strVal < *other.strVal {
			return -1
		} else if *c.strVal > *other.strVal {
			return 1
		} else {
			return 0
		}
	}
	return -1 // types don't match
}
