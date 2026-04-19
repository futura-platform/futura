package futura

import (
	"context"

	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/moment"
)

// FlowBuilder is a builder for a flow. It is used as a helper for more readable flow definitions.
// It implements context.Context.
type FlowBuilder struct {
	unexportedContext
	execution *execution.FlowExecution
}

func newFlowBuilder(ctx context.Context, exec *execution.FlowExecution) FlowBuilder {
	return FlowBuilder{
		unexportedContext: unexportedContext{
			Context: ctx,
		},
		execution: exec,
	}
}

type unexportedContext struct {
	context.Context
}

var _ context.Context = FlowBuilder{}

// WithKey attaches a "key" to the returned FlowBuilder.
// This is useful for looping flows, in order to identify the current iteration.
// (similar to "key" in React)
func (b FlowBuilder) WithKey(key string) FlowBuilder {
	return b.WithContext(moment.WithIdentityKey(b, key))
}

func (b FlowBuilder) WithContext(ctx context.Context) FlowBuilder {
	b.unexportedContext = unexportedContext{
		Context: ctx,
	}
	return b
}
