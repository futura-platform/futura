package durable

import (
	"context"
	"errors"
	"sync"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/internal/flowhooks"
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

// Handles is the execution's cache of resolved handles, one per key, in the order they were provided.
type Handles struct {
	mu    sync.Mutex
	byKey map[HandleKey]any
	order []HandleKey
}

// LoadOrCompute returns the handle cached under key, computing and caching it if there is none.
func (h *Handles) LoadOrCompute(key HandleKey, compute func() any) any {
	h.mu.Lock()
	defer h.mu.Unlock()
	if handle, ok := h.byKey[key]; ok {
		return handle
	}
	handle := compute()
	h.byKey[key] = handle
	h.order = append(h.order, key)
	return handle
}

// Cleanup releases every cached handle that has something to release, in LIFO order.
func (h *Handles) Cleanup() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	var err error
	for i := len(h.order) - 1; i >= 0; i-- {
		if cleaner, ok := h.byKey[h.order[i]].(Cleaner); ok {
			err = errors.Join(err, cleaner.Cleanup())
		}
	}
	return err
}

// WithHandlesCache gives the execution a cache of resolved handles.
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
		})(context.WithValue(ctx, handlesKey, &Handles{byKey: map[HandleKey]any{}}))
	}
}

func GetHandles(ctx context.Context) (*Handles, bool) {
	h, ok := ctx.Value(handlesKey).(*Handles)
	return h, ok
}

// CleanupHandles cleans up every cached handle that has something to release.
func CleanupHandles(ctx context.Context) error {
	handles, ok := GetHandles(ctx)
	if !ok {
		return nil
	}
	return handles.Cleanup()
}
