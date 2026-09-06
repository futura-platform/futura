package durable

import (
	"context"
	"errors"
	"sync"

	"github.com/futura-platform/futura/internal/errors"
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
	// Flush returns the handle's value if it differs from the committed one.
	Flush() (value []byte, changed bool)
	// OnCommitted is called once value has been committed.
	OnCommitted(value []byte)
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

// Flush returns the value of every cached handle that differs from the committed one, by key.
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

// OnCommitted is called once the values returned by Flush have been committed.
func (h *Handles) OnCommitted(flushed map[string][]byte) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for key, value := range flushed {
		h.byKey[HandleKey(key)].OnCommitted(value)
	}
}

// Cleanup releases every cached handle in LIFO order, including the ones a cleanup resolves.
func (h *Handles) Cleanup() error {
	var err error
	for {
		h.mu.Lock()
		if len(h.order) == 0 {
			h.mu.Unlock()
			return err
		}
		last := h.order[len(h.order)-1]
		h.order = h.order[:len(h.order)-1]
		handle := h.byKey[last]
		h.mu.Unlock()
		err = errors.Join(err, ftrerrors.Recovering(handle.Cleanup))
	}
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
