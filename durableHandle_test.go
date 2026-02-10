package futura

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/flow/execution"
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

func TestDurableHandle(t *testing.T) {
	expectedValue := byte(100)
	handle := NewDurableHandle("firstHandle",
		func() *byte { return &expectedValue },
		func(input []byte) (*byte, error) { return &input[0], nil },
		func(*byte) ([]byte, error) { return []byte{expectedValue}, nil },
		nil,
	)

	t.Run("panics use is called before anything is provided", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, ErrDurableResolverNotFound, func() {
			handle.Use(newFlowBuilder(t.Context(), execution.NewFlowExecution()))
		})
	})

	t.Run("panics if the context already has a value for the key", func(t *testing.T) {
		exec := execution.NewFlowExecution()
		ctx := handle.Provide()(execution.WithFlow(t.Context(), exec))
		testutil.PanicsWithErrorIs(t, ErrDurableResolverAlreadyProvided, func() {
			handle.Provide()(ctx)
		})
	})

	t.Run("panics if another handle with the same key is used to access the value", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := handle.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		otherHandle := NewDurableHandle[byte]("firstHandle", nil, nil, nil, nil)
		testutil.PanicsWithErrorIs(t, ErrDurableResolverMismatch, func() { otherHandle.Use(b) })
	})

	t.Run("returns the correct value", func(t *testing.T) {
		c := executiontype.NewInMemoryContainer()
		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := handle.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		ref, persist := handle.Use(b)
		assert.Equal(t, expectedValue, *ref)

		// does not store anything until persisted
		_, ok, err := c.LoadDurable("firstHandle")
		assert.NoError(t, err)
		assert.False(t, ok)

		// returns the correct value after a change
		*ref = byte(101)
		persist()

		value, ok, err := c.LoadDurable("firstHandle")
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

		execCopy := execution.NewFlowExecutionWithContainer(copy)
		ctxCopy := handle.Provide()(execution.WithFlow(t.Context(), execCopy))
		bCopy := newFlowBuilder(ctxCopy, execCopy)
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
		err := inner.StoreDurable(durableKey, []byte{byte(42)})
		assert.NoError(t, err)

		counting := &storeCountingContainer{inner: inner}
		exec := execution.NewFlowExecutionWithContainer(counting)
		ctx := h.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		ref, persist := h.Use(b)
		assert.Equal(t, byte(42), *ref)

		// no change is a no-op
		didChange := persist()
		assert.False(t, didChange)
		assert.Equal(t, int32(0), counting.storeCalls.Load())

		value, ok, err := inner.LoadDurable(durableKey)
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

		value, ok, err = inner.LoadDurable(durableKey)
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, []byte{byte(43)}, value)
	})

	t.Run("uses constructor when missing, unmarshal when present", func(t *testing.T) {
		constructorCalls := 0
		unmarshalCalls := 0
		marshalCalls := 0

		callsHandle := NewDurableHandle("callsHandle",
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
		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := callsHandle.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

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
		err := c.StoreDurable(durableKey, []byte{byte(42)})
		assert.NoError(t, err)

		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := h.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

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
		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := h.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		ref, _ := h.Use(b)
		*ref = 9

		// Get a new persist func after mutation; this must still store.
		_, persist2 := h.Use(b)
		didChange := persist2()
		assert.True(t, didChange)

		serialized, ok, err := c.LoadDurable(durableKey)
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
		exec := execution.NewFlowExecutionWithContainer(counting)
		ctx := h.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

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

		errHandle := NewDurableHandle("unmarshalErrHandle",
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
		err := c.StoreDurable("unmarshalErrHandle", []byte{123})
		assert.NoError(t, err)
		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := errHandle.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		testutil.PanicsWithErrorIs(t, expectedErr, func() { errHandle.Use(b) })
		assert.Equal(t, 0, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)
	})

	t.Run("panics if marshal returns an error", func(t *testing.T) {
		expectedErr := errors.New("marshal failed")
		marshalCalls := 0

		errHandle := NewDurableHandle("marshalErrHandle",
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
		exec := execution.NewFlowExecutionWithContainer(c)
		ctx := errHandle.Provide()(execution.WithFlow(t.Context(), exec))
		b := newFlowBuilder(ctx, exec)

		_, persist := errHandle.Use(b)
		testutil.PanicsWithErrorIs(t, expectedErr, func() { persist() })
		assert.Equal(t, 1, marshalCalls)

		_, ok, err := c.LoadDurable("marshalErrHandle")
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

		r, err := NewFlow[*any, string]().Execute(
			t.Context(),
			func(b FlowBuilder, _ *any) (string, error) {
				ref, _ := h.Use(b)
				usedPtr = ref
				*ref = 7
				return "ok", nil
			},
			nil,
			h.Provide(),
		)
		assert.NoError(t, err)
		assert.Equal(t, "ok", r)
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

		_, err := NewFlow[*any, string]().Execute(
			t.Context(),
			func(b FlowBuilder, _ *any) (string, error) {
				return "ok", nil
			},
			nil,
			h.Provide(),
		)
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

		_, err := NewFlow[*any, string]().Execute(
			t.Context(),
			func(b FlowBuilder, _ *any) (string, error) {
				_, _ = h.Use(b)
				return "cancelled", ftype.ErrCancelFlow
			},
			nil,
			h.Provide(),
		)
		assert.ErrorIs(t, err, ftype.ErrCancelFlow)
		assert.Equal(t, 1, cleanupCalls)
	})
}
