package ftype

import "runtime"

type MomentFnMetadata struct {
	Label       string
	RuntimeFunc *runtime.Func
}

type MomentFnOption func(*MomentFnMetadata)

func WithLabel(label string) MomentFnOption {
	return func(m *MomentFnMetadata) {
		m.Label = label
	}
}

// WithRuntimeFunc sets the function a moment is recorded and validated against.
// Helpers that wrap a user's function in their own must pass the user's, so that the
// moment is bound to the function the user wrote rather than to the wrapper.
func WithRuntimeFunc(fn *runtime.Func) MomentFnOption {
	return func(m *MomentFnMetadata) {
		m.RuntimeFunc = fn
	}
}
