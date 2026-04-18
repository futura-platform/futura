package step

import (
	"context"

	mapset "github.com/deckarep/golang-set/v2"
)

type contextKey string

const (
	activeGoroutinesContextKey contextKey = "active_goroutines"
)

func withActiveGoroutines(ctx context.Context) (mapset.Set[int64], context.Context) {
	s := mapset.NewSet[int64]()
	return s, context.WithValue(ctx, activeGoroutinesContextKey, s)
}

func ActiveGoroutinesFrom(ctx context.Context) mapset.Set[int64] {
	return ctx.Value(activeGoroutinesContextKey).(mapset.Set[int64])
}
