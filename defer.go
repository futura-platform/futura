package futura

import "github.com/futura-platform/futura/internal/flow/replay/sequence"

// Defer registers a function that will be called when the flow ends.
// It has LIFO semantics, the same as Go's official defer statement.
func Defer(b FlowBuilder, fn func()) {
	sequence.Defer(b, fn)
}
