package futura

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flowhooks"
	"github.com/futura-platform/futura/internal/goroutinebind"
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

type durableResolver[T any] struct {
	handleId int32

	// durableMu protects the resolver state below. It is shared across resolve and
	// persist calls, so that remote checksum state stays canonical across multiple
	// Use() calls (and multiple persist funcs).
	durableMu sync.Mutex

	valueLoader  sync.Once
	valueLoadErr any
	cached       fastComparableValue[T]
}

type syncLevel int

const (
	syncLevelNone syncLevel = iota
	syncLevelLocal
	syncLevelRemote
)

type fastComparableValue[T any] struct {
	value    *T
	checksum uint64
	sync     syncLevel
}

func (r *durableResolver[T]) resolve(ctx context.Context, d *DurableHandle[T]) *T {
	r.durableMu.Lock()
	defer r.durableMu.Unlock()

	r.valueLoader.Do(func() {
		defer func() {
			if rr := recover(); rr != nil {
				slog.Error("failed to load durable value", "error", rr)
				r.valueLoadErr = rr
			}
		}()

		exec := execution.MustFromContext(ctx)
		serialized, ok := exec.LoadDurable(ctx, string(d.key))

		var v fastComparableValue[T]
		if !ok {
			v.value = d.constructor()
			v.checksum = 0
			v.sync = syncLevelLocal
		} else {
			value, err := d.unmarshal(serialized)
			if err != nil {
				panic(err)
			}
			v.value = value
			v.checksum = xxhash.Sum64(serialized)
			v.sync = syncLevelRemote
		}

		r.cached = v
	})
	if r.valueLoadErr != nil {
		panic(r.valueLoadErr)
	}

	return r.cached.value
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

	// either load the existing resolver, or create a new one
	anyResolver, _ := handles.LoadOrCompute(d.key, func() (any, bool) {
		return &durableResolver[T]{
			handleId: d.id,
		}, false
	})
	resolver := anyResolver.(*durableResolver[T])

	// Optionally register cleanup to run at execution end.
	ctx = flowhooks.WithOnExecutionEnd(func(ctx context.Context, _ error) error {
		if d.cleanup == nil {
			return nil
		}

		resolver.durableMu.Lock()
		v := resolver.cached.value
		resolver.durableMu.Unlock()
		if v == nil {
			return nil
		}
		return d.cleanup(v)
	})(ctx)
	return context.WithValue(ctx, d.key, resolver)
}

var (
	ErrDurableResolverNotFound = errors.New("durable resolver not found")
	ErrDurableResolverMismatch = errors.New("durable resolver mismatch")
)

// Use returns a reference to the durable value, and a function to persist the changes to the durable value.
// Persist should ALWAYS be called at some point after the ref is mutated.
// Persist will return true if the value was changed, and false if it was not.
// If persist returns false, that also implies that the StoreDurable call was skipped.
// Not doing so can cause the changes to be lost in the event of a failure.
// This function will attempt to load the value from the execution container,
// and if it fails, it will call the constructor to create a new value. (via the durableResolver).
func (d *DurableHandle[T]) Use(ctx context.Context) (ref *T, persist func() (didChange bool)) {
	// first check if the builder context has a value for this key
	r, ok := ctx.Value(d.key).(*durableResolver[T])
	if !ok {
		panic(fmt.Errorf("%w: %s", ErrDurableResolverNotFound, d.key))
	} else if r.handleId != d.id {
		panic(fmt.Errorf("%w: %s, expected %d, got %d", ErrDurableResolverMismatch, d.key, d.id, r.handleId))
	}

	ref = r.resolve(ctx, d)
	return ref, func() bool {

		serialized, err := d.marshal(ref)
		if err != nil {
			panic(err)
		}

		// don't call store if the value hasn't changed
		localChecksum := xxhash.Sum64(serialized)

		// persist is callable anywhere, so we need to temporarily bind to the current goroutine to allow the store to happen.
		ctx = goroutinebind.BindGoroutine(ctx)

		r.durableMu.Lock()
		defer r.durableMu.Unlock()
		didChange := r.cached.sync != syncLevelRemote || localChecksum != r.cached.checksum
		if didChange {
			exec := execution.MustFromContext(ctx)
			exec.StoreDurable(ctx, string(d.key), serialized)
			// update remote state so repeated persist calls are idempotent.
			r.cached.checksum = localChecksum
			r.cached.sync = syncLevelRemote
		}
		return didChange
	}
}
