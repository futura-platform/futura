package executiontype

import "github.com/futura-platform/futura/moment"

// State is the in memory state of the flow execution.
// All values here should be usable between program instances (i.e. no unsafe pointers, functions, goroutine ids, etc.).
// This type is designed to be serialized and deserialized to facilitate distributed execution.
type State struct {
	// a map of the step moment identifiers to their memoized moment.
	memoTable map[moment.Identity]moment.Moment

	// given a certain state, this sequence should be deterministic.
	callOrder []moment.Identity
}

func NewState() State {
	return State{
		memoTable: make(map[moment.Identity]moment.Moment),
		callOrder: make([]moment.Identity, 0),
	}
}
