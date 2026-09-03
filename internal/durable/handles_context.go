package durable

import (
	"context"
	"errors"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flowhooks"
	"github.com/puzpuzpuz/xsync/v4"
)

type handlesContextKey string

const handlesKey handlesContextKey = "durable_handles"

var (
	ErrHandlesAlreadyExists = errors.New("handles already exists")
)

type HandleKey string

// Cleaner is implemented by a cached handle that has something to release when the execution ends.
type Cleaner interface {
	Cleanup() error
}

// WithHandlesCache gives the execution a cache of resolved handles, one per handle key.
// The cache is what the execution cleans up at its end, so a handle's cleanup runs regardless
// of where in the execution the handle was provided.
func WithHandlesCache() ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		_, alreadyExists := GetHandles(ctx)
		if alreadyExists {
			panic(ErrHandlesAlreadyExists)
		}

		return flowhooks.WithOnExecutionEnd(func(ctx context.Context, _ error) error {
			return CleanupHandles(ctx)
		})(context.WithValue(ctx, handlesKey, xsync.NewMap[HandleKey, any]()))
	}
}

func GetHandles(ctx context.Context) (*xsync.Map[HandleKey, any], bool) {
	m, ok := ctx.Value(handlesKey).(*xsync.Map[HandleKey, any])
	return m, ok
}

// CleanupHandles cleans up every cached handle that has something to release.
func CleanupHandles(ctx context.Context) error {
	handles, ok := GetHandles(ctx)
	if !ok {
		return nil
	}
	var err error
	handles.Range(func(_ HandleKey, handle any) bool {
		if cleaner, ok := handle.(Cleaner); ok {
			err = errors.Join(err, cleaner.Cleanup())
		}
		return true
	})
	return err
}
