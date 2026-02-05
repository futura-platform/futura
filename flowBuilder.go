package futura

import (
	"context"

	"github.com/futura-platform/futura/moment"
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

// WithKey attaches a "key" to the returned FlowBuilder.
// This is useful for looping flows, in order to identify the current iteration.
// (similar to "key" in React)
func (b FlowBuilder) WithKey(key any) FlowBuilder {
	return b.WithContextWrapper(func(ctx context.Context) context.Context {
		return moment.WithIdentityKey(ctx, key)
	})
}

func (b FlowBuilder) WithContextWrapper(wrapper func(context.Context) context.Context) FlowBuilder {
	return FlowBuilder{
		unexportedContext: unexportedContext{
			Context: wrapper(b),
		},
	}
}
