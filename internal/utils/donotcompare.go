package utils

// DoNotCompare prevents struct comparison, forcing errors.Is to use the Is() method.
// See: https://go.dev/blog/module-compatibility
type Donotcompare [0]func()
