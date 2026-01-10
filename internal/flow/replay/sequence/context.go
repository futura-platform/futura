package sequence

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay"
)

type contextKey string

const ctxKey contextKey = "sequence_index_context"

type state struct {
	flags         replay.Flags
	index         int
	seenCallpaths mapset.Set[moment.Identity]
}

// With wraps the context with a new sequence index.
func With(ctx context.Context, flags replay.Flags) context.Context {
	return context.WithValue(ctx, ctxKey, &state{
		flags:         flags,
		seenCallpaths: mapset.NewSet[moment.Identity](),
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

func MarkSeen(ctx context.Context, identity moment.Identity) {
	s := getSequenceState(ctx)
	s.seenCallpaths.Add(identity)
}

func IsSeen(ctx context.Context, identity moment.Identity) bool {
	s := getSequenceState(ctx)
	return s.seenCallpaths.Contains(identity)
}

func GetFlags(ctx context.Context) replay.Flags {
	return getSequenceState(ctx).flags
}
