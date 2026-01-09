package sequence

import "context"

type contextKey string

const ctxKey contextKey = "sequence_index_context"

// With wraps the context with a new sequence index.
func With(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKey, new(int))
}

func getIndexPtr(ctx context.Context) *int {
	index, ok := ctx.Value(ctxKey).(*int)
	if !ok {
		panic("sequence index not found")
	}
	return index
}

func GetIndex(ctx context.Context) int {
	return *getIndexPtr(ctx)
}

func Advance(ctx context.Context) {
	i := getIndexPtr(ctx)
	*i++
}
