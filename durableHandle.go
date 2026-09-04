package futura

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/flow/execution"
)

type DurableHandle[T any] struct {
	id  int32
	key durable.HandleKey

	constructor func() *T
	unmarshal   func([]byte) (*T, error)
	marshal     func(*T) ([]byte, error)
	cleanup     func(*T) error
}

var handleSequence atomic.Int32

// NewDurableHandle creates a new durable handle.
// This is designed to be used as a singleton, at 1 or more scopes above the Flow Execute call.
// The key is used to identify the value within the task's execution container.
// The same DurableHandle key may only be used at most once per flow.
// Values are stored in the consuming flow's execution container.
// constructor is called to create a new value when no value is found in the execution container.
// unmarshal is called to deserialize the value from the execution container.
// marshal is called to serialize the value to the execution container.
// cleanup is called to clean up the value when the flow stops execution. It is only called if the value is not nil.
// all four of these method MUST NOT use the handle it belongs to, it will deadlock for all but cleanup.
func NewDurableHandle[T any](
	key string,
	constructor func() *T,
	unmarshal func([]byte) (*T, error),
	marshal func(*T) ([]byte, error),
	cleanup func(*T) error,
) *DurableHandle[T] {
	return &DurableHandle[T]{
		id:          handleSequence.Add(1),
		key:         durable.HandleKey(key),
		constructor: constructor,
		unmarshal:   unmarshal,
		marshal:     marshal,
		cleanup:     cleanup,
	}
}

// durableResolver is a handle's value within one execution: resolved once, mutated in place by steps,
// and flushed at every durable boundary.
type durableResolver[T any] struct {
	handleId int32
	marshal  func(*T) ([]byte, error)
	cleanup  func(*T) error

	valueLoader  sync.Once
	valueLoadErr any
	// mu guards value and flushed: resolve and Flush can run on different goroutines.
	mu    sync.Mutex
	value *T
	// flushed is the value as last loaded or staged, so that an unchanged value is not stored again.
	flushed []byte
}

// Cleanup implements durable.Cleaner. It is only called if the value was resolved.
func (r *durableResolver[T]) Cleanup() error {
	if r.cleanup == nil {
		return nil
	}
	r.mu.Lock()
	v := r.value
	r.mu.Unlock()
	if v == nil {
		return nil
	}
	return r.cleanup(v)
}

// Flush implements durable.Flusher.
func (r *durableResolver[T]) Flush() (value []byte, changed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.value == nil {
		return nil, false
	}
	serialized, err := r.marshal(r.value)
	if err != nil {
		panic(err)
	}
	if bytes.Equal(serialized, r.flushed) {
		return nil, false
	}
	r.flushed = serialized
	return serialized, true
}

func (r *durableResolver[T]) resolve(ctx context.Context, d *DurableHandle[T]) *T {
	r.valueLoader.Do(func() {
		defer func() {
			if rr := recover(); rr != nil {
				slog.Error("failed to load durable value", "error", rr)
				r.valueLoadErr = rr
			}
		}()

		exec := execution.MustFromContext(ctx)
		serialized, ok := exec.LoadDurable(ctx, string(d.key))
		var value *T
		if !ok {
			value = d.constructor()
		} else {
			var err error
			if value, err = d.unmarshal(serialized); err != nil {
				panic(err)
			}
		}

		r.mu.Lock()
		defer r.mu.Unlock()
		r.value = value
		r.flushed = serialized
	})
	if r.valueLoadErr != nil {
		panic(r.valueLoadErr)
	}
	return r.value
}

func (d *DurableHandle[T]) Key() string {
	return string(d.key)
}

func (d *DurableHandle[T]) Marshal(v *T) ([]byte, error) {
	return d.marshal(v)
}

func (d *DurableHandle[T]) Unmarshal(data []byte) (*T, error) {
	return d.unmarshal(data)
}

var (
	ErrDurableResolverAlreadyProvided = errors.New("durable resolver already provided")
	ErrDurableHandlesNotFound         = errors.New("durable handles not found")
)

// Provide is a convenience wrapper with the same semantics as ProvideContext, but for FlowBuilder.
// This function should be used in flow functions.
func (d *DurableHandle[T]) Provide(b FlowBuilder) FlowBuilder {
	// provide the resolver to the flow
	return b.WithContext(d.ProvideContext(b))
}

// ProvideContext wraps the context with a context that will provide the durable resolver to the flow.
// Once a value pointer is resolved either via loading from the execution container or via the constructor,
// it is cached and will be returned by subsequent calls to the resolver.
// If this cached pointer is non nil by the time execution ends, it will be passed into a cleanup function call.
// As opposed to Provide, this function should be used when a FlowBuilder is not available, like in a stateful ftype.FlowLoopOption. (fopt/with_max_failures.go uses this)
func (d *DurableHandle[T]) ProvideContext(ctx context.Context) context.Context {
	// first check if the context already has a value for this key
	if ctx.Value(d.key) != nil {
		panic(fmt.Errorf("%w: %s", ErrDurableResolverAlreadyProvided, d.key))
	}

	// first load the handle cache
	handles, ok := durable.GetHandles(ctx)
	if !ok {
		panic(fmt.Errorf("%w: %s", ErrDurableHandlesNotFound, d.key))
	}
	exec := execution.MustFromContext(ctx)
	if handles != exec.Handles() {
		// the context's cache belongs to an execution that ended: nothing flushes or cleans it up any more
		panic(fmt.Errorf("%w: %s", ErrDurableResolverStale, d.key))
	}

	// either load the existing resolver, or create a new one.
	// the cache cleans its resolvers up when the execution ends.
	resolver := handles.LoadOrCompute(d.key, func() durable.Handle {
		return &durableResolver[T]{
			handleId: d.id,
			marshal:  d.marshal,
			cleanup:  d.cleanup,
		}
	})
	return context.WithValue(ctx, d.key, resolver.(*durableResolver[T]))
}

var (
	ErrDurableResolverNotFound = errors.New("durable resolver not found")
	ErrDurableResolverMismatch = errors.New("durable resolver mismatch")
	ErrDurableResolverStale    = errors.New("durable resolver belongs to an execution that has ended")
)

// Use returns the durable value, to be mutated in place inside steps.
// Every change is committed with the memo of the step that made it, so nothing has to be called for
// a change to be durable. The value is loaded from the execution container the first time it is used
// in an execution, and constructed if the container has none.
func (d *DurableHandle[T]) Use(ctx context.Context) *T {
	r, ok := ctx.Value(d.key).(*durableResolver[T])
	if !ok {
		panic(fmt.Errorf("%w: %s", ErrDurableResolverNotFound, d.key))
	} else if r.handleId != d.id {
		panic(fmt.Errorf("%w: %s, expected %d, got %d", ErrDurableResolverMismatch, d.key, d.id, r.handleId))
	} else if handles, _ := durable.GetHandles(ctx); handles != execution.MustFromContext(ctx).Handles() {
		// the value belongs to an execution that ended: it was cleaned up, and its changes no longer flush
		panic(fmt.Errorf("%w: %s", ErrDurableResolverStale, d.key))
	}
	return r.resolve(ctx, d)
}
