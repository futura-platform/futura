package futura

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/durable"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flowhooks"
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

func (tx *storeCountingTx) StoreDurable(key string, value []byte) error {
	tx.storeCalls.Add(1)
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

func newDurableTestBuilder(t *testing.T, exec *execution.FlowExecution, providers ...func(FlowBuilder) FlowBuilder) FlowBuilder {
	t.Helper()
	startExecRun(t, exec)
	ctx := durable.WithHandlesCache()(execution.WithFlow(t.Context(), exec))
	b := newFlowBuilder(ctx, exec)
	for _, provide := range providers {
		b = provide(b)
	}
	return b
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

		ref, persist := handle.Use(b)
		assert.Equal(t, expectedValue, *ref)

		// does not store anything until persisted
		_, ok, err := c.LoadDurable(execution.GenericDurableKey("firstHandle"))
		assert.NoError(t, err)
		assert.False(t, ok)

		// returns the correct value after a change
		*ref = byte(101)
		persist()

		value, ok, err := c.LoadDurable(execution.GenericDurableKey("firstHandle"))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(101)}, value)

		ref, _ = handle.Use(b)
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
		ref, _ = handle.Use(bCopy)
		assert.Equal(t, byte(101), *ref)
	})

	t.Run("persist only stores when value changes", func(t *testing.T) {
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

		ref, persist := h.Use(b)
		assert.Equal(t, byte(42), *ref)

		// no change is a no-op
		didChange := persist()
		assert.False(t, didChange)
		assert.Equal(t, int32(0), counting.storeCalls.Load())

		value, ok, err := inner.LoadDurable(execution.GenericDurableKey(durableKey))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(42)}, value)

		// change stores once, then becomes a no-op
		*ref = byte(43)

		didChange = persist()
		assert.True(t, didChange)
		assert.Equal(t, int32(1), counting.storeCalls.Load())

		didChange = persist()
		assert.False(t, didChange)
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

		ref1, persist := callsHandle.Use(b)
		assert.Equal(t, 7, *ref1)
		assert.Equal(t, 1, constructorCalls)
		assert.Equal(t, 0, unmarshalCalls)
		assert.Equal(t, 0, marshalCalls)

		persist()
		assert.Equal(t, 1, marshalCalls)

		ref2, _ := callsHandle.Use(b)
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
		flowCtx := durable.WithHandlesCache()(execution.WithFlow(t.Context(), exec))

		replayOneBuilder := h.Provide(newFlowBuilder(flowCtx, exec))
		refOne, _ := h.Use(replayOneBuilder)
		*refOne = 22

		replayTwoBuilder := h.Provide(newFlowBuilder(flowCtx, exec))
		refTwo, _ := h.Use(replayTwoBuilder)

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

		ref1, _ := h.Use(b)
		assert.Equal(t, 42, *ref1)
		assert.Equal(t, 0, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)

		ref2, _ := h.Use(b)
		assert.Same(t, ref1, ref2)
		assert.Equal(t, 42, *ref2)
		assert.Equal(t, 0, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)
	})

	t.Run("persist should store if value changed even when using a new persist func", func(t *testing.T) {
		const durableKey = "newPersistFuncStores"

		h := NewDurableHandle[int](
			durableKey,
			func() *int {
				v := 0
				return &v
			},
			func(input []byte) (*int, error) {
				v := int(input[0])
				return &v, nil
			},
			func(v *int) ([]byte, error) { return []byte{byte(*v)}, nil },
			nil,
		)

		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref, _ := h.Use(b)
		*ref = 9

		// Get a new persist func after mutation; this must still store.
		_, persist2 := h.Use(b)
		didChange := persist2()
		assert.True(t, didChange)

		serialized, ok, err := c.LoadDurable(execution.GenericDurableKey(durableKey))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(9)}, serialized)
	})

	t.Run("persist remains optimized across multiple Use calls", func(t *testing.T) {
		const durableKey = "persistOptimizationAcrossUse"

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
		counting := &storeCountingContainer{inner: inner}
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(counting))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref1, persist1 := h.Use(b)
		*ref1 = byte(1)
		assert.True(t, persist1())
		assert.Equal(t, int32(1), counting.storeCalls.Load())

		// New persist func, no further changes => no extra store.
		_, persist2 := h.Use(b)
		assert.False(t, persist2())
		assert.Equal(t, int32(1), counting.storeCalls.Load())
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

	t.Run("persist can be called from a different goroutine than Use", func(t *testing.T) {
		// The persist closure must be safely callable from any goroutine, even
		// though the underlying execution context is bound to the goroutine
		// that created it. persist re-binds the context to its own goroutine
		// before touching the execution container; without that rebind,
		// MustFromContext would panic via AssertBoundGoroutine.
		const durableKey = "persistCrossGoroutineHandle"

		h := NewDurableHandle(
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

		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref, persist := h.Use(b)
		*ref = byte(77)

		var (
			didChange     bool
			persistErr    any
			persistDoneWg sync.WaitGroup
		)
		persistDoneWg.Go(func() {
			assert.NotPanics(t, func() {
				didChange = persist()
			})
		})
		persistDoneWg.Wait()

		assert.Nil(t, persistErr, "persist should not panic when called from a different goroutine")
		assert.True(t, didChange)

		value, ok, err := c.LoadDurable(execution.GenericDurableKey(durableKey))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(77)}, value)
	})

	t.Run("persist panics if invoked after the execution has stopped running", func(t *testing.T) {
		// The persist closure becomes unsafe to call once the FlowExecution it
		// belongs to has stopped running, because anything stored at that point
		// would escape the cleanup hook and silently corrupt the container for
		// any subsequent replay. The check lives in execution.FromContext via
		// the running flag; this test exercises that path through persist.
		const durableKey = "persistAfterStopHandle"

		h := NewDurableHandle(
			durableKey,
			func() *byte { v := byte(0); return &v },
			func(input []byte) (*byte, error) { v := input[0]; return &v, nil },
			func(v *byte) ([]byte, error) { return []byte{*v}, nil },
			nil,
		)

		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(containertest.NewStrict(c))
		stop, ok := exec.TryStartRun()
		assert.True(t, ok)
		ctx := durable.WithHandlesCache()(execution.WithFlow(t.Context(), exec))
		b := h.Provide(newFlowBuilder(ctx, exec))

		ref, persist := h.Use(b)
		*ref = byte(1)
		assert.True(t, persist())

		stop()
		*ref = byte(2)
		testutil.PanicsWithErrorIs(t, execution.ErrFlowExecutionNotRunning, func() {
			persist()
		})

		stored, ok, err := c.LoadDurable(execution.GenericDurableKey(durableKey))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(1)}, stored,
			"the post-stop persist must not have stored the new value")
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

		_, persist := errHandle.Use(b)
		testutil.PanicsWithErrorIs(t, expectedErr, func() { persist() })
		assert.Equal(t, 1, marshalCalls)

		_, ok, err := c.LoadDurable(execution.GenericDurableKey("marshalErrHandle"))
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestDurableHandle_Cleanup(t *testing.T) {
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
		ref, _ := h.Use(b)
		usedPtr = ref
		*ref = 7

		err := flowhooks.RunOnExecutionEnd(b, nil)
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
		err := flowhooks.RunOnExecutionEnd(b, nil)
		assert.NoError(t, err)
		assert.Equal(t, 0, cleanupCalls)
	})

	t.Run("calls cleanup even when execution ends with ErrCancelFlow", func(t *testing.T) {
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
		_, _ = h.Use(b)

		err := flowhooks.RunOnExecutionEnd(b, ftype.ErrCancelFlow)
		assert.NoError(t, err)
		assert.Equal(t, 1, cleanupCalls)
	})
}

func TestDurableHandle_Persist(t *testing.T) {
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

	t.Run("persist survives the container retrying the transaction", func(t *testing.T) {
		h := newByteHandle("persistRetryingHandle")
		c := executiontype.NewInMemoryContainer()
		strict := containertest.NewStrict(c)
		exec := execution.NewFlowExecutionWithContainer(strict)
		b := newDurableTestBuilder(t, exec, h.Provide)

		ref, persist := h.Use(b)
		*ref = byte(5)
		assert.True(t, persist())

		value, ok := storedValue(t, c, h)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(5)}, value)
	})
}
