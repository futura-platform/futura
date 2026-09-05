package futura

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
)

type storeCountingContainer struct {
	inner *executiontype.InMemoryContainer

	storeCalls atomic.Int32
}

type storeCountingTx struct {
	*executiontype.InMemoryContainer
	storeCalls *atomic.Int32
}

// StoreDurable counts the stores of handle values; a durable boundary also writes the execution's epochs.
func (tx *storeCountingTx) StoreDurable(key string, value []byte) error {
	if strings.HasPrefix(key, execution.GenericDurableKey("")) {
		tx.storeCalls.Add(1)
	}
	return tx.InMemoryContainer.StoreDurable(key, value)
}

func (c *storeCountingContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	return fn(ctx, &storeCountingTx{
		InMemoryContainer: c.inner,
		storeCalls:        &c.storeCalls,
	})
}

func (c *storeCountingContainer) ReadTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.ReadOnlyContainer) error) error {
	return fn(ctx, c.inner)
}

var errRejected = errors.New("rejected")

// rejectingContainer rejects write transactions while reject is set, the way a backend does on a transient error.
type rejectingContainer struct {
	*executiontype.InMemoryContainer
	reject bool
}

func (c *rejectingContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	if c.reject {
		return errRejected
	}
	return c.InMemoryContainer.Transact(ctx, fn)
}

func newDurableTestBuilder(t *testing.T, exec *execution.FlowExecution, providers ...func(FlowBuilder) FlowBuilder) FlowBuilder {
	t.Helper()
	startExecRun(t, exec)
	ctx := durable.WithHandles(execution.WithFlow(t.Context(), exec), exec.Handles())
	b := newFlowBuilder(ctx, exec)
	for _, provide := range providers {
		b = provide(b)
	}
	return b
}

// boundary reaches a durable boundary without a loop: it starts a replay on exec, which commits every
// pending change the same way a step's memo does.
func boundary(exec *execution.FlowExecution, ctx context.Context) {
	exec.StartNewReplay(ctx)
}

// startExecRun marks exec as running for the duration of the test, so that
// FromContext-based access works in tests that bypass the production Loop path.
func startExecRun(t *testing.T, exec *execution.FlowExecution) {
	t.Helper()
	stop, ok := exec.TryStartRun()
	if !ok {
		t.Fatalf("exec is already running")
	}
	t.Cleanup(stop)
}

func TestDurableHandle(t *testing.T) {
	expectedValue := byte(100)
	handle := NewDurableHandle[byte]("firstHandle",
		func() *byte { return &expectedValue },
		func(input []byte) (*byte, error) { return &input[0], nil },
		func(*byte) ([]byte, error) { return []byte{expectedValue}, nil },
		nil,
	)

	t.Run("panics use is called before anything is provided", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, ErrDurableResolverNotFound, func() {
			handle.Use(newFlowBuilder(t.Context(), execution.NewFlowExecutionWithContainer(containertest.NewInMemory())))
		})
	})

	t.Run("panics if provide is called without handles cache", func(t *testing.T) {
		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		b := newFlowBuilder(execution.WithFlow(t.Context(), exec), exec)
		testutil.PanicsWithErrorIs(t, ErrDurableHandlesNotFound, func() {
			handle.Provide(b)
		})
	})

	t.Run("panics if the context already has a value for the key", func(t *testing.T) {
		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		b := newDurableTestBuilder(t, exec, handle.Provide)
		testutil.PanicsWithErrorIs(t, ErrDurableResolverAlreadyProvided, func() {
			handle.Provide(b)
		})
	})

	t.Run("panics if another handle with the same key is used to access the value", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, handle.Provide)

		otherHandle := NewDurableHandle[byte]("firstHandle", nil, nil, nil, nil)
		testutil.PanicsWithErrorIs(t, ErrDurableResolverMismatch, func() { otherHandle.Use(b) })
	})

	t.Run("returns the correct value", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, handle.Provide)

		ref := handle.Use(b)
		assert.Equal(t, expectedValue, *ref)

		// does not store anything until a durable boundary
		_, ok, err := c.LoadDurable(execution.GenericDurableKey("firstHandle"))
		assert.NoError(t, err)
		assert.False(t, ok)

		// returns the correct value after a change
		*ref = byte(101)
		boundary(exec, b)

		value, ok, err := c.LoadDurable(execution.GenericDurableKey("firstHandle"))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(101)}, value)

		ref = handle.Use(b)
		assert.Equal(t, byte(101), *ref)

		// returns the correct value in a copy of the previous execution container
		var buf bytes.Buffer
		encoder := privateencoding.NewEncoder[*executiontype.InMemoryContainer](&buf)
		encoder.Encode(c)
		decoder := privateencoding.NewDecoder[*executiontype.InMemoryContainer](&buf)
		copy, err := decoder.Decode()
		assert.NoError(t, err)

		execCopy := execution.NewFlowExecutionWithContainer(containertest.NewStrict(copy))
		bCopy := newDurableTestBuilder(t, execCopy, handle.Provide)
		ref = handle.Use(bCopy)
		assert.Equal(t, byte(101), *ref)
	})

	t.Run("a boundary only stores the value when it changed", func(t *testing.T) {
		const durableKey = "persistOptimizationHandle"

		h := NewDurableHandle[byte](
			durableKey,
			func() *byte {
				v := byte(0)
				return &v
			},
			func(input []byte) (*byte, error) {
				v := input[0]
				return &v, nil
			},
			func(v *byte) ([]byte, error) { return []byte{*v}, nil },
			nil,
		)

		inner := executiontype.NewInMemoryContainer()
		err := inner.StoreDurable(execution.GenericDurableKey(durableKey), []byte{byte(42)})
		assert.NoError(t, err)

		counting := &storeCountingContainer{inner: inner}
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(counting))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref := h.Use(b)
		assert.Equal(t, byte(42), *ref)

		// no change is a no-op
		boundary(exec, b)
		assert.Equal(t, int32(0), counting.storeCalls.Load())

		value, ok, err := inner.LoadDurable(execution.GenericDurableKey(durableKey))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(42)}, value)

		// change stores once, then becomes a no-op
		*ref = byte(43)

		boundary(exec, b)
		assert.Equal(t, int32(1), counting.storeCalls.Load())

		boundary(exec, b)
		assert.Equal(t, int32(1), counting.storeCalls.Load())

		value, ok, err = inner.LoadDurable(execution.GenericDurableKey(durableKey))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(43)}, value)
	})

	t.Run("uses constructor when missing, unmarshal when present", func(t *testing.T) {
		constructorCalls := 0
		unmarshalCalls := 0
		marshalCalls := 0

		callsHandle := NewDurableHandle[int]("callsHandle",
			func() *int {
				constructorCalls++
				v := 7
				return &v
			},
			func(input []byte) (*int, error) {
				unmarshalCalls++
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) {
				marshalCalls++
				return []byte{byte(*v)}, nil
			},
			nil,
		)

		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, callsHandle.Provide)

		ref1 := callsHandle.Use(b)
		assert.Equal(t, 7, *ref1)
		assert.Equal(t, 1, constructorCalls)
		assert.Equal(t, 0, unmarshalCalls)
		assert.Equal(t, 0, marshalCalls)

		boundary(exec, b)
		assert.Equal(t, 1, marshalCalls)

		ref2 := callsHandle.Use(b)
		assert.Same(t, ref1, ref2)
		assert.Equal(t, 7, *ref2)
		assert.Equal(t, 1, constructorCalls)
		assert.Equal(t, 0, unmarshalCalls)
		assert.Equal(t, 1, marshalCalls)
	})

	t.Run("reuses constructor result across replay contexts sharing handle cache", func(t *testing.T) {
		constructorCalls := 0
		h := NewDurableHandle[int](
			"constructorAcrossReplay",
			func() *int {
				constructorCalls++
				v := 21
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			nil,
		)

		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		startExecRun(t, exec)
		flowCtx := durable.WithHandles(execution.WithFlow(t.Context(), exec), exec.Handles())

		replayOneBuilder := h.Provide(newFlowBuilder(flowCtx, exec))
		refOne := h.Use(replayOneBuilder)
		*refOne = 22

		replayTwoBuilder := h.Provide(newFlowBuilder(flowCtx, exec))
		refTwo := h.Use(replayTwoBuilder)

		assert.Same(t, refOne, refTwo)
		assert.Equal(t, 22, *refTwo)
		assert.Equal(t, 1, constructorCalls)
	})

	t.Run("caches the resolved pointer when present (unmarshal runs once)", func(t *testing.T) {
		constructorCalls := 0
		unmarshalCalls := 0

		const durableKey = "presentCacheHandle"
		h := NewDurableHandle[int](
			durableKey,
			func() *int {
				constructorCalls++
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				unmarshalCalls++
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			nil,
		)

		c := executiontype.NewInMemoryContainer()
		err := c.StoreDurable(execution.GenericDurableKey(durableKey), []byte{byte(42)})
		assert.NoError(t, err)

		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref1 := h.Use(b)
		assert.Equal(t, 42, *ref1)
		assert.Equal(t, 0, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)

		ref2 := h.Use(b)
		assert.Same(t, ref1, ref2)
		assert.Equal(t, 42, *ref2)
		assert.Equal(t, 0, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)
	})

	t.Run("panics if unmarshal returns an error", func(t *testing.T) {
		expectedErr := errors.New("unmarshal failed")
		constructorCalls := 0
		unmarshalCalls := 0

		errHandle := NewDurableHandle[byte]("unmarshalErrHandle",
			func() *byte {
				constructorCalls++
				v := byte(1)
				return &v
			},
			func([]byte) (*byte, error) {
				unmarshalCalls++
				return nil, expectedErr
			},
			func(v *byte) ([]byte, error) { return []byte{*v}, nil },
			nil,
		)

		c := executiontype.NewInMemoryContainer()
		err := c.StoreDurable(execution.GenericDurableKey("unmarshalErrHandle"), []byte{123})
		assert.NoError(t, err)
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, errHandle.Provide)

		testutil.PanicsWithErrorIs(t, expectedErr, func() { errHandle.Use(b) })
		assert.Equal(t, 0, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)
	})

	t.Run("a load that fails is retried by the next use", func(t *testing.T) {
		expectedErr := errors.New("unmarshal failed")
		unmarshalCalls := 0
		h := NewDurableHandle[byte]("retriedLoadHandle",
			func() *byte { v := byte(1); return &v },
			func(input []byte) (*byte, error) {
				unmarshalCalls++
				if unmarshalCalls == 1 {
					return nil, expectedErr
				}
				v := input[0]
				return &v, nil
			},
			func(v *byte) ([]byte, error) { return []byte{*v}, nil },
			nil,
		)
		c := executiontype.NewInMemoryContainer()
		assert.NoError(t, c.StoreDurable(execution.GenericDurableKey("retriedLoadHandle"), []byte{42}))
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, h.Provide)

		testutil.PanicsWithErrorIs(t, expectedErr, func() { h.Use(b) })
		assert.Equal(t, byte(42), *h.Use(b))
		assert.Equal(t, 2, unmarshalCalls)
	})

	t.Run("panics if marshal returns an error", func(t *testing.T) {
		expectedErr := errors.New("marshal failed")
		marshalCalls := 0

		errHandle := NewDurableHandle[byte]("marshalErrHandle",
			func() *byte {
				v := byte(1)
				return &v
			},
			func([]byte) (*byte, error) {
				t.Fatalf("unmarshal should not be called")
				return nil, nil
			},
			func(*byte) ([]byte, error) {
				marshalCalls++
				return nil, expectedErr
			},
			nil,
		)

		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, errHandle.Provide)

		errHandle.Use(b)
		testutil.PanicsWithErrorIs(t, expectedErr, func() { boundary(exec, b) })
		assert.Equal(t, 1, marshalCalls)

		_, ok, err := c.LoadDurable(execution.GenericDurableKey("marshalErrHandle"))
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestDurableHandle_Cleanup(t *testing.T) {
	t.Run("handles are cleaned up in reverse order of being provided", func(t *testing.T) {
		var order []string
		newHandle := func(name string) *DurableHandle[int] {
			return NewDurableHandle[int](name,
				func() *int { v := 0; return &v },
				func(input []byte) (*int, error) { v := int(input[0]); return &v, nil },
				func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
				func(*int) error { order = append(order, name); return nil },
			)
		}
		first, second, third := newHandle("first"), newHandle("second"), newHandle("third")

		for range 20 { // an unordered cache would pass by chance on one run
			order = nil
			_, err := NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
				b = first.Provide(b)
				b = second.Provide(b)
				b = third.Provide(b)
				first.Use(b)
				second.Use(b)
				third.Use(b)
				return 0, nil
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, []string{"third", "second", "first"}, order)
		}
	})

	t.Run("a cleanup that panics does not stop the others, and is reported", func(t *testing.T) {
		var order []string
		newHandle := func(name string, cleanup func() error) *DurableHandle[int] {
			return NewDurableHandle[int](name,
				func() *int { v := 0; return &v },
				func(input []byte) (*int, error) { v := int(input[0]); return &v, nil },
				func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
				func(*int) error { return cleanup() },
			)
		}
		first := newHandle("panickingCleanupFirst", func() error { order = append(order, "first"); return nil })
		second := newHandle("panickingCleanupSecond", func() error { panic("second cleanup panics") })
		third := newHandle("panickingCleanupThird", func() error { order = append(order, "third"); return nil })
		_, err := NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
			b = third.Provide(second.Provide(first.Provide(b)))
			first.Use(b)
			second.Use(b)
			third.Use(b)
			return 0, nil
		}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorContains(t, err, "second cleanup panics")
		assert.Equal(t, []string{"third", "first"}, order)
	})

	t.Run("a cleanup can use another handle", func(t *testing.T) {
		// cleanup runs user code, so it must not run under the cache's lock: a cleanup that records
		// what it did in another handle provides that handle through the same cache
		audit := NewPlainDurableHandle("cleanupAudit", func() *int { v := 0; return &v })
		var flowCtx context.Context
		conn := NewDurableHandle[int]("cleanupUsesAnotherHandle",
			func() *int { v := 0; return &v },
			func(input []byte) (*int, error) { v := int(input[0]); return &v, nil },
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(*int) error {
				*audit.Use(audit.ProvideContext(flowCtx))++
				return nil
			},
		)

		done := make(chan error, 1)
		go func() {
			_, err := NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
				b = conn.Provide(b)
				flowCtx = b
				conn.Use(b)
				return 0, nil
			}, struct{}{})
			done <- err
		}()
		select {
		case err := <-done:
			assert.NoError(t, err)
		case <-time.After(3 * time.Second):
			t.Fatal("the execution never ended: the cleanup blocked on the handles cache")
		}
	})
	t.Run("a builder from a previous execution cannot use a handle", func(t *testing.T) {
		// the value a stale builder would hand back belongs to an execution that ended: it was cleaned
		// up, and its changes no longer flush
		h := NewPlainDurableHandle("staleUse", func() *int { v := 0; return &v })
		f := NewFlowFromContainer[struct{}, int](containertest.NewInMemory())
		var stale *FlowBuilder
		flowFn := func(b FlowBuilder, _ struct{}) (int, error) {
			b = h.Provide(b)
			if err := Action(b, func(ctx context.Context) error { *h.Use(b)++; return nil }); err != nil {
				return 0, err
			}
			if stale != nil {
				testutil.PanicsWithErrorIs(t, ErrDurableResolverStale, func() { h.Use(*stale) })
			}
			stale = &b
			return *h.Use(b), nil
		}
		_, err := f.Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		_, err = f.Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
	})
	t.Run("a builder from a previous execution cannot provide a handle", func(t *testing.T) {
		h := NewPlainDurableHandle("staleProvide", func() *int { v := 0; return &v })
		f := NewFlowFromContainer[struct{}, int](containertest.NewInMemory())
		var stale FlowBuilder
		_, err := f.Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
			stale = b
			return 0, nil
		}, struct{}{})
		assert.NoError(t, err)

		_, err = f.Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
			testutil.PanicsWithErrorIs(t, ErrDurableResolverStale, func() { h.Provide(stale) })
			return 0, nil
		}, struct{}{})
		assert.NoError(t, err)
	})
	t.Run("handles can be provided from a goroutine bound to the execution", func(t *testing.T) {
		a := NewPlainDurableHandle("providedFromGoroutine", func() *int { v := 0; return &v })
		c := NewPlainDurableHandle("providedFromFlow", func() *int { v := 0; return &v })
		cleanups := 0
		aCleanup := NewDurableHandle[int]("providedFromGoroutineCleanup",
			func() *int { v := 0; return &v },
			func(input []byte) (*int, error) { v := int(input[0]); return &v, nil },
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(*int) error { cleanups++; return nil },
		)
		_, err := NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
			return 0, Action(b, func(ctx context.Context) error {
				var wg sync.WaitGroup
				wg.Go(func() {
					gctx, done := BindToGoroutine(ctx)
					defer done()
					ref := a.Use(a.ProvideContext(gctx))
					*ref = 1
					aCleanup.Use(aCleanup.ProvideContext(gctx))
				})
				ref := c.Use(c.ProvideContext(ctx))
				*ref = 2
				wg.Wait()
				return nil
			})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, cleanups, "a handle provided from the goroutine is still cleaned up")
	})

	t.Run("runs once at the end of an execution when the handle is provided inside the flow", func(t *testing.T) {
		cleanupCalls, replays := 0, 0
		var cleaned *int
		h := NewDurableHandle[int](
			"cleanupProvidedInFlowHandle",
			func() *int {
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(v *int) error {
				cleanupCalls++
				cleaned = v
				return nil
			},
		)

		r, err := NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
			replays++
			b = h.Provide(b)
			ref := h.Use(b)
			if replays == 1 {
				*ref = 7
				// replay once more, so that cleanup is proven to run per execution rather than per replay
				return 0, Action(b, func(ctx context.Context) error { return errors.New("retry") })
			}
			return *ref, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 7, r)
		assert.Equal(t, 2, replays)
		assert.Equal(t, 1, cleanupCalls)
		assert.Equal(t, 7, *cleaned)
	})

	t.Run("runs once at the end of an execution when the handle is provided as a loop option", func(t *testing.T) {
		cleanupCalls := 0
		h := NewDurableHandle[int](
			"cleanupProvidedAsOptionHandle",
			func() *int {
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(*int) error {
				cleanupCalls++
				return nil
			},
		)

		var optionCtx context.Context
		_, err := NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b FlowBuilder, _ struct{}) (int, error) {
			h.Use(optionCtx)
			return 0, nil
		}, struct{}{}, func(ctx context.Context) context.Context {
			optionCtx = h.ProvideContext(ctx)
			return optionCtx
		})
		assert.NoError(t, err)
		assert.Equal(t, 1, cleanupCalls)
	})

	t.Run("calls cleanup with the cached pointer at execution end", func(t *testing.T) {
		cleanupCalls := 0
		var (
			usedPtr    *int
			cleanedPtr *int
		)

		h := NewDurableHandle[int](
			"cleanupHandle",
			func() *int {
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(v *int) error {
				cleanupCalls++
				cleanedPtr = v
				return nil
			},
		)

		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		b := newDurableTestBuilder(t, exec, h.Provide)
		ref := h.Use(b)
		usedPtr = ref
		*ref = 7

		err := execution.MustFromContext(b).Handles().Cleanup()
		assert.NoError(t, err)
		assert.Equal(t, 1, cleanupCalls)
		assert.Same(t, usedPtr, cleanedPtr)
		assert.Equal(t, 7, *cleanedPtr)
	})

	t.Run("does not call cleanup if the handle is never used", func(t *testing.T) {
		cleanupCalls := 0
		h := NewDurableHandle[int](
			"cleanupUnusedHandle",
			func() *int {
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(*int) error {
				cleanupCalls++
				return nil
			},
		)

		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		b := newDurableTestBuilder(t, exec, h.Provide)
		err := execution.MustFromContext(b).Handles().Cleanup()
		assert.NoError(t, err)
		assert.Equal(t, 0, cleanupCalls)
	})

	t.Run("calls cleanup once the handle has been used", func(t *testing.T) {
		cleanupCalls := 0
		h := NewDurableHandle[int](
			"cleanupOnCancelHandle",
			func() *int {
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			func(*int) error {
				cleanupCalls++
				return nil
			},
		)

		exec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		b := newDurableTestBuilder(t, exec, h.Provide)
		h.Use(b)

		err := execution.MustFromContext(b).Handles().Cleanup()
		assert.NoError(t, err)
		assert.Equal(t, 1, cleanupCalls)
	})
}

func TestDurableHandle_Boundary(t *testing.T) {
	newByteHandle := func(key string) *DurableHandle[byte] {
		return NewDurableHandle[byte](key,
			func() *byte {
				v := byte(0)
				return &v
			},
			func(input []byte) (*byte, error) {
				v := input[0]
				return &v, nil
			},
			func(v *byte) ([]byte, error) { return []byte{*v}, nil },
			nil,
		)
	}

	// storedValue reads the handle's stored bytes straight from the container.
	storedValue := func(t *testing.T, c *executiontype.InMemoryContainer, h *DurableHandle[byte]) ([]byte, bool) {
		t.Helper()
		value, ok, err := c.LoadDurable(execution.GenericDurableKey(h.Key()))
		assert.NoError(t, err)
		return value, ok
	}

	t.Run("a change whose boundary fails to commit is committed by the next boundary", func(t *testing.T) {
		h := newByteHandle("boundaryFailingHandle")
		c := &rejectingContainer{InMemoryContainer: executiontype.NewInMemoryContainer()}
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref := h.Use(b)
		*ref = byte(5)
		c.reject = true
		testutil.PanicsWithErrorIs(t, execution.ErrTransactionFailed, func() { boundary(exec, b) })
		_, ok := storedValue(t, c.InMemoryContainer, h)
		assert.False(t, ok)

		c.reject = false
		boundary(exec, b)
		value, ok := storedValue(t, c.InMemoryContainer, h)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(5)}, value)
	})

	t.Run("an empty value is stored the first time", func(t *testing.T) {
		h := NewDurableHandle[[]string]("boundaryEmptyHandle",
			func() *[]string { v := []string{"default"}; return &v },
			func(input []byte) (*[]string, error) { v := strings.Split(string(input), ","); return &v, nil },
			func(v *[]string) ([]byte, error) { return []byte(strings.Join(*v, ",")), nil },
			nil,
		)
		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, h.Provide)

		*h.Use(b) = nil
		boundary(exec, b)
		value, ok, err := c.LoadDurable(execution.GenericDurableKey(h.Key()))
		assert.NoError(t, err)
		assert.True(t, ok, "the container had nothing under the key, so even an empty value is a change")
		assert.Empty(t, value)
	})

	t.Run("a marshal that reuses its buffer stores every change", func(t *testing.T) {
		buffer := make([]byte, 1)
		h := NewDurableHandle[byte]("boundaryReusedBufferHandle",
			func() *byte { v := byte(0); return &v },
			func(input []byte) (*byte, error) { v := input[0]; return &v, nil },
			func(v *byte) ([]byte, error) { buffer[0] = *v; return buffer, nil },
			nil,
		)
		counting := &storeCountingContainer{inner: executiontype.NewInMemoryContainer()}
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(counting))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref := h.Use(b)
		for _, v := range []byte{1, 2} {
			*ref = v
			boundary(exec, b)
		}
		assert.Equal(t, int32(2), counting.storeCalls.Load())
		value, ok := storedValue(t, counting.inner, h)
		assert.True(t, ok)
		assert.Equal(t, []byte{2}, value)
	})

	t.Run("a value that aliases the loaded bytes stores its changes", func(t *testing.T) {
		h := NewDurableHandle[[]byte]("boundaryAliasingHandle",
			func() *[]byte { v := []byte{}; return &v },
			func(input []byte) (*[]byte, error) { return &input, nil },
			func(v *[]byte) ([]byte, error) { return *v, nil },
			nil,
		)
		inner := executiontype.NewInMemoryContainer()
		assert.NoError(t, inner.StoreDurable(execution.GenericDurableKey(h.Key()), []byte("ab")))
		counting := &storeCountingContainer{inner: inner}
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(counting))
		b := newDurableTestBuilder(t, exec, h.Provide)

		(*h.Use(b))[0] = 'x'
		boundary(exec, b)
		assert.Equal(t, int32(1), counting.storeCalls.Load(), "the change went unnoticed")
	})

	t.Run("a change survives the container retrying the transaction", func(t *testing.T) {
		h := newByteHandle("boundaryRetryingHandle")
		c := executiontype.NewInMemoryContainer()
		strict := containertest.NewStrict(c)
		exec := execution.NewFlowExecutionWithContainer(strict)
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref := h.Use(b)
		*ref = byte(5)
		boundary(exec, b)

		value, ok := storedValue(t, c, h)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(5)}, value)
	})
}
