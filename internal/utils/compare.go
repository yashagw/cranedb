package utils

// CompareValues compares two values of potentially different types.
// Returns -1 if v1 < v2, 0 if equal, 1 if v1 > v2.
// Handles int, string
func CompareValues(v1, v2 any) int {
	// Handle int comparison
	if i1, ok1 := v1.(int); ok1 {
		if i2, ok2 := v2.(int); ok2 {
			if i1 < i2 {
				return -1
			} else if i1 > i2 {
				return 1
			}
			return 0
		}
	}

	// Handle string comparison
	if s1, ok1 := v1.(string); ok1 {
		if s2, ok2 := v2.(string); ok2 {
			if s1 < s2 {
				return -1
			} else if s1 > s2 {
				return 1
			}
			return 0
		}
	}

	// If we can't compare, return 0 (treat as equal)
	// This shouldn't happen in normal operation
	return 0
}
