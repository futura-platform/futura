package futura

import "context"

type ctxKey int

const (
	flowContextKey ctxKey = iota
)

// A "moment" represents a function and its returned successful result at a specific point in time.
type moment struct {
	fnPointer uintptr

	result any
}

type flowContext struct {
	memoizedMomentSequence []moment
	sequenceIndex          int
}

func withFlow(ctx context.Context) context.Context {
	return context.WithValue(ctx, flowContextKey, &flowContext{})
}

func getFlowContext(ctx context.Context) (*flowContext, bool) {
	v, ok := ctx.Value(flowContextKey).(*flowContext)
	return v, ok
}

func mustGetFlowContext(ctx context.Context) *flowContext {
	f, ok := getFlowContext(ctx)
	if !ok {
		panic("flowContext not found in context")
	}
	return f
}
