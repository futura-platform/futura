package futura

import (
	"context"

	"github.com/futura-platform/futura/internal/flow/moment"
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

func (b FlowBuilder) WithKey(key any) FlowBuilder {
	return FlowBuilder{
		unexportedContext: unexportedContext{
			Context: moment.WithIdentityKey(b, key),
		},
	}
}
