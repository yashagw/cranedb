//go:build release

package failpoint

// Enable is a no-op in release builds.
func Enable(name, value string) {}

// Disable is a no-op in release builds.
func Disable(name string) {}

// Inject does nothing in release builds.
func Inject(name string, ctx interface{}, fn func(val string, ctx interface{})) {}

// InjectPanic does nothing in release builds.
func InjectPanic(name string, ctx interface{}) {}

// GetCount returns 0 in release builds.
func GetCount(name string) int { return 0 }

// ClearAll disables all failpoints and resets counters.
func ClearAll() {
	// No-op in release builds
}

// Context is a helper type for passing context to Inject.
type Context map[string]interface{}

// With creates a new failpoint Context with the given key-value pair.
func With(key string, value interface{}) Context {
	return Context{key: value}
}

// With adds a key-value pair to an existing Context.
func (c Context) With(key string, value interface{}) Context {
	c[key] = value
	return c
}
