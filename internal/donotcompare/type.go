package donotcompare

// doNotCompare prevents struct comparison, forcing errors.Is to use the Is() method.
// See: https://go.dev/blog/module-compatibility
type T [0]func()
