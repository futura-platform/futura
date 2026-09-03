package sequence

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/moment"
)

type contextKey string

const ctxKey contextKey = "sequence_index_context"

type state struct {
	flags    replay.Flags
	index    int
	seen     mapset.Set[moment.Identity]
	deferred *func()
	// failed is set once a step in this replay has not completed. Nothing after it may evaluate.
	failed bool
}

// With wraps the context with a new sequence index.
func With(ctx context.Context, flags replay.Flags) context.Context {
	deferred := func() {}
	return context.WithValue(ctx, ctxKey, &state{
		flags:    flags,
		seen:     mapset.NewSet[moment.Identity](),
		deferred: &deferred,
	})
}

func getSequenceState(ctx context.Context) *state {
	state, ok := ctx.Value(ctxKey).(*state)
	if !ok {
		panic("sequence state not found")
	}
	return state
}

func GetIndex(ctx context.Context) int {
	return getSequenceState(ctx).index
}

func Advance(ctx context.Context) {
	s := getSequenceState(ctx)
	s.index++
}

// MarkFailed records that a step in this replay did not complete.
func MarkFailed(ctx context.Context) {
	getSequenceState(ctx).failed = true
}

// HasFailed reports whether a step in this replay did not complete.
func HasFailed(ctx context.Context) bool {
	return getSequenceState(ctx).failed
}

func GetFlags(ctx context.Context) replay.Flags {
	return getSequenceState(ctx).flags
}

func MarkSeen(ctx context.Context, identity moment.Identity) {
	s := getSequenceState(ctx)
	s.seen.Add(identity)
}

func IsSeen(ctx context.Context, identity moment.Identity) bool {
	s := getSequenceState(ctx)
	return s.seen.Contains(identity)
}

// Defer registers a function that will be called exactly once when the flow ends.
// It has LIFO semantics, the same as Go's official defer statement.
func Defer(ctx context.Context, fn func()) {
	s := getSequenceState(ctx)
	lastDeferred := *s.deferred
	*s.deferred = func() {
		fn()
		lastDeferred()
	}
}

// RunDeferred runs all deferred functions in reverse order of registration (LIFO).
func RunDeferred(ctx context.Context) {
	(*getSequenceState(ctx).deferred)()
}
