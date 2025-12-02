package query

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
)

// Constant represents either an integer, string, or boolean constant value.
type Constant struct {
	intVal  *int
	strVal  *string
	boolVal *bool
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

// NewBoolConstant creates a new Constant with a boolean value.
func NewBoolConstant(val bool) *Constant {
	return &Constant{
		boolVal: &val,
	}
}

// String returns a string representation of the constant.
func (c *Constant) String() string {
	if c.intVal != nil {
		return fmt.Sprintf("%d", *c.intVal)
	}
	if c.boolVal != nil {
		return fmt.Sprintf("%t", *c.boolVal)
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

// AsBool returns the boolean value of the constant.
func (c *Constant) AsBool() bool {
	return *c.boolVal
}

// equals checks if the constant is equal to another constant.
func (c *Constant) Equals(other *Constant) bool {
	if c.intVal != nil && other.intVal != nil {
		return *c.intVal == *other.intVal
	}
	if c.strVal != nil && other.strVal != nil {
		return *c.strVal == *other.strVal
	}
	if c.boolVal != nil && other.boolVal != nil {
		return *c.boolVal == *other.boolVal
	}
	return false
}

// compareTo returns -1, 0, or 1 if this Constant is less than, equal to, or greater than the other, respectively.
// Returns -1 if types do not match.
// For booleans: false < true
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
	if c.boolVal != nil && other.boolVal != nil {
		if !*c.boolVal && *other.boolVal {
			return -1 // false < true
		} else if *c.boolVal && !*other.boolVal {
			return 1 // true > false
		} else {
			return 0 // equal
		}
	}
	return -1 // types don't match
}

// IsInt returns true if the constant holds an integer value.
func (c *Constant) IsInt() bool {
	return c.intVal != nil
}

// IsString returns true if the constant holds a string value.
func (c *Constant) IsString() bool {
	return c.strVal != nil
}

// IsBool returns true if the constant holds a boolean value.
func (c *Constant) IsBool() bool {
	return c.boolVal != nil
}

// Hash returns a hash of the constant.
func (c *Constant) Hash() int {
	hasher := fnv.New64a()

	if c.intVal != nil {
		var buf [9]byte
		buf[0] = 0x01
		binary.LittleEndian.PutUint64(buf[1:], uint64(int64(*c.intVal)))
		_, _ = hasher.Write(buf[:])
	} else if c.boolVal != nil {
		var buf [2]byte
		buf[0] = 0x03
		if *c.boolVal {
			buf[1] = 0x01
		} else {
			buf[1] = 0x00
		}
		_, _ = hasher.Write(buf[:])
	} else {
		_, _ = hasher.Write([]byte{0x02})
		_, _ = hasher.Write([]byte(*c.strVal))
	}

	return int(hasher.Sum64())
}
