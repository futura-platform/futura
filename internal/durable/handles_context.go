package durable

import (
	"context"
	"errors"
	"sync"

	ftrerrors "github.com/futura-platform/futura/internal/errors"
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

// Flusher is implemented by a cached handle whose value is committed at the execution's durable boundaries.
type Flusher interface {
	// Flush returns the handle's value if it changed since it was last flushed.
	Flush() (value []byte, changed bool)
}

// Handle is a resolved handle: what the cache flushes at every durable boundary and cleans up at the end.
type Handle interface {
	Cleaner
	Flusher
}

// Handles is an execution's cache of resolved handles, one per key, in the order they were provided.
// The execution flushes it at every durable boundary, and cleans it up at its end.
type Handles struct {
	mu    sync.Mutex
	byKey map[HandleKey]Handle
	order []HandleKey
}

func NewHandles() *Handles {
	return &Handles{byKey: map[HandleKey]Handle{}}
}

// LoadOrCompute returns the handle cached under key, computing and caching it if there is none.
func (h *Handles) LoadOrCompute(key HandleKey, compute func() Handle) Handle {
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

// Flush returns the value of every cached handle that changed since it was last flushed, by key.
func (h *Handles) Flush() map[string][]byte {
	h.mu.Lock()
	defer h.mu.Unlock()
	changed := map[string][]byte{}
	for _, key := range h.order {
		if value, ok := h.byKey[key].Flush(); ok {
			changed[string(key)] = value
		}
	}
	return changed
}

// Cleanup releases every cached handle that has something to release, in LIFO order.
func (h *Handles) Cleanup() error {
	h.mu.Lock()
	toCleanup := make([]Handle, 0, len(h.order))
	for i := len(h.order) - 1; i >= 0; i-- {
		toCleanup = append(toCleanup, h.byKey[h.order[i]])
	}
	h.mu.Unlock()

	var err error
	for _, handle := range toCleanup {
		err = errors.Join(err, ftrerrors.Recovering(handle.Cleanup))
	}
	return err
}

// WithHandles puts an execution's cache of handles on ctx, for handles to resolve themselves through.
func WithHandles(ctx context.Context, handles *Handles) context.Context {
	if _, alreadyExists := GetHandles(ctx); alreadyExists {
		panic(ErrHandlesAlreadyExists)
	}
	return context.WithValue(ctx, handlesKey, handles)
}

func GetHandles(ctx context.Context) (*Handles, bool) {
	h, ok := ctx.Value(handlesKey).(*Handles)
	return h, ok
}
