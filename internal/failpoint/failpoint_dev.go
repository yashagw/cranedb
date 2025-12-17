//go:build !release

package failpoint

import (
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
)

var (
	// mu protects the failpoints map and counters
	mu sync.RWMutex
	// failpoints stores the active failpoints and their values
	failpoints = make(map[string]string)
	// counters tracks how many times each failpoint has been called
	counters = make(map[string]int)
)

// Enable activates a failpoint with a given value.
// The value can be:
//   - Empty string: failpoint triggers immediately
//   - A number (e.g., "3"): failpoint triggers after being called that many times
//   - "count=N" format: same as above, more explicit
func Enable(name, value string) {
	mu.Lock()
	defer mu.Unlock()
	failpoints[name] = value
	counters[name] = 0 // Reset counter when enabling
	fmt.Printf("DEBUG: Failpoint enabled: %s (value: %s)\n", name, value)
}

// Disable deactivates a failpoint.
func Disable(name string) {
	mu.Lock()
	defer mu.Unlock()
	delete(failpoints, name)
	delete(counters, name)
	fmt.Printf("DEBUG: Failpoint disabled: %s\n", name)
}

// Inject checks if a failpoint is active and runs the callback if it is.
func Inject(name string, ctx interface{}, fn func(val string, ctx interface{})) {
	mu.Lock()
	defer mu.Unlock()

	val, ok := failpoints[name]
	if !ok {
		return // Failpoint not enabled
	}

	// Increment counter
	counters[name]++

	if evaluate(val, name, ctx) {
		if fn != nil {
			fn(val, ctx)
		}
	}
}

// InjectPanic checks if a failpoint is active and panics with a standard message if it is.
// The panic message format is "failpoint: crash <name>".
func InjectPanic(name string, ctx interface{}) {
	Inject(name, ctx, func(val string, ctx interface{}) {
		slog.Error("Failpoint hit", "name", name)
		panic("failpoint: crash " + name)
	})
}

// evaluate checks if the failpoint condition matches the context and context.
func evaluate(condition, name string, ctx interface{}) bool {
	if condition == "" {
		fmt.Printf("DEBUG: Failpoint hit (global): %s\n", name)
		return true
	}
	if condition == "return" {
		fmt.Printf("DEBUG: Failpoint hit (return): %s\n", name)
		return true
	}

	// Split by space for AND logic
	parts := strings.Fields(condition)
	for _, part := range parts {
		if !checkCondition(part, name, ctx) {
			return false // One condition didn't match
		}
	}
	// All matched
	return true
}

func checkCondition(condition, name string, ctx interface{}) bool {
	// 1. Check numeric/count conditions
	// Format: "N" or "count=N" or "count>N" etc.
	if isCountCondition(condition) {
		return evalCount(condition, name)
	}

	// 2. Check context matching
	// Format: "key=value"
	if strings.Contains(condition, "=") {
		parts := strings.SplitN(condition, "=", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Check if ctx is a map
		var ctxMap map[string]interface{}
		if m, ok := ctx.(map[string]interface{}); ok {
			ctxMap = m
		} else if m, ok := ctx.(Context); ok {
			ctxMap = m
		}

		if ctxMap != nil {
			if ctxVal, exists := ctxMap[key]; exists {
				ctxValStr := fmt.Sprintf("%v", ctxVal)
				if ctxValStr == value {
					fmt.Printf("DEBUG: Failpoint matched part (context %s=%s): %s\n", key, value, name)
					return true
				}
			}
		}
	}

	return false
}

func isCountCondition(cond string) bool {
	if _, err := strconv.Atoi(cond); err == nil {
		return true
	}
	return strings.HasPrefix(cond, "count")
}

func evalCount(cond, name string) bool {
	count := counters[name]

	// Simple number "N" -> "count=N"
	if target, err := strconv.Atoi(cond); err == nil {
		if count == target {
			fmt.Printf("DEBUG: Failpoint hit (count=%d): %s\n", count, name)
			return true
		}
		return false
	}

	// Parse "countOPvalue"
	// Supported ops: =, >, <, >=, <=
	cond = strings.ReplaceAll(cond, "count", "")

	var op string
	var threshold int
	var err error

	if strings.HasPrefix(cond, ">=") {
		op = ">="
		threshold, err = strconv.Atoi(cond[2:])
	} else if strings.HasPrefix(cond, "<=") {
		op = "<="
		threshold, err = strconv.Atoi(cond[2:])
	} else if strings.HasPrefix(cond, ">") {
		op = ">"
		threshold, err = strconv.Atoi(cond[1:])
	} else if strings.HasPrefix(cond, "<") {
		op = "<"
		threshold, err = strconv.Atoi(cond[1:])
	} else if strings.HasPrefix(cond, "=") {
		op = "="
		threshold, err = strconv.Atoi(cond[1:])
	} else {
		return false // Invalid syntax
	}

	if err != nil {
		fmt.Printf("DEBUG: Failpoint invalid count syntax '%s': %s\n", cond, err)
		return false
	}

	match := false
	switch op {
	case "=":
		match = count == threshold
	case ">":
		match = count > threshold
	case "<":
		match = count < threshold
	case ">=":
		match = count >= threshold
	case "<=":
		match = count <= threshold
	}

	if match {
		fmt.Printf("DEBUG: Failpoint hit (count %s %d): %s\n", op, threshold, name)
	}
	return match
}

// GetCount returns the current call count for a failpoint (useful for testing/debugging).
func GetCount(name string) int {
	mu.RLock()
	defer mu.RUnlock()
	return counters[name]
}

// ClearAll disables all failpoints and resets counters.
func ClearAll() {
	mu.Lock()
	defer mu.Unlock()
	failpoints = make(map[string]string)
	counters = make(map[string]int)
}

// Context is a helper type for passing context to Inject.
type Context map[string]interface{}

// With creates a new failpoint Context with the given key-value pair.
func With(key string, value interface{}) Context {
	return Context{key: value}
}

// With adds a key-value pair to an existing Context.
// Useful for chaining: failpoint.With("k1", v1).With("k2", v2)
func (c Context) With(key string, value interface{}) Context {
	c[key] = value
	return c
}
