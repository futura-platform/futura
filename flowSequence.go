package futura

import "context"

type FlowSequenceOptions struct {
	// OnError is called when an error is encountered in any step of the flow.
	OnError func(err error) (continueExecution bool)
}

// ExecuteFlow executes the flow fn. It expects fn to be pure, except in child Step functions.
// It will continuously retry the flow until it is without error or the context is done.
func ExecuteFlow[T comparable](ctx context.Context, opts *FlowSequenceOptions, fn func(ctx context.Context) (T, error)) (T, error) {
	if opts == nil {
		opts = &FlowSequenceOptions{}
	}

	ctx = withFlow(ctx)
	f := mustGetFlowContext(ctx)
	for {
		result, err := fn(ctx)
		if err == nil {
			return result, nil
		} else if ctx.Err() != nil {
			// if the context is done, comply by returning immediately
			return result, ctx.Err()
		}

		// if we encounter any other error, handle it and then rewind the sequence to the start
		if opts.OnError != nil {
			continueExecution := opts.OnError(err)
			if !continueExecution {
				return result, err
			}
		}
		f.sequenceIndex = 0
	}
}
