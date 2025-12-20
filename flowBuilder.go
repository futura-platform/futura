package futura

import (
	"context"
)

// FlowBuilder is a builder for a flow. It is used as a helper for more readable flow definitions.
// It implements context.Context.
type FlowBuilder struct {
	unexportedContext
}

type unexportedContext struct {
	context.Context
}

var _ context.Context = FlowBuilder{}
