package futura

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

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
	)

	t.Run("panics use is called before anything is provided", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, ErrDurableResolverNotFound, func() {
			handle.Use(newFlowBuilder(t.Context(), execution.NewFlowExecution()))
		})
	})

	ctx := handle.Provide()(t.Context())

	t.Run("panics if the context already has a value for the key", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, ErrDurableResolverAlreadyProvided, func() {
			handle.Provide()(ctx)
		})
	})

	c := executiontype.NewInMemoryContainer()
	b := newFlowBuilder(ctx, execution.NewFlowExecutionWithContainer(c))

	t.Run("panics if another handle with the same key is used to access the value", func(t *testing.T) {
		otherHandle := NewDurableHandle[byte]("firstHandle", nil, nil, nil)
		testutil.PanicsWithErrorIs(t, ErrDurableResolverMismatch, func() { otherHandle.Use(b) })
	})

	t.Run("returns the correct value", func(t *testing.T) {
		ref, persist := handle.Use(b)
		assert.Equal(t, expectedValue, *ref)

		t.Run("does not store anything until persisted", func(t *testing.T) {
			_, ok, err := c.LoadDurable("firstHandle")
			assert.NoError(t, err)
			assert.False(t, ok)
		})

		t.Run("returns the correct value after a change", func(t *testing.T) {
			*ref = byte(101)
			persist()

			t.Run("stores the serialized value in the container", func(t *testing.T) {
				value, ok, err := c.LoadDurable("firstHandle")
				assert.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, []byte{byte(101)}, value)
			})

			ref, _ = handle.Use(b)
			assert.Equal(t, byte(101), *ref)
		})

		t.Run("returns the correct value in a copy of the previous execution container", func(t *testing.T) {
			var buf bytes.Buffer
			encoder := privateencoding.NewEncoder[*executiontype.InMemoryContainer](&buf)
			encoder.Encode(c)
			decoder := privateencoding.NewDecoder[*executiontype.InMemoryContainer](&buf)
			copy, err := decoder.Decode()
			assert.NoError(t, err)

			ctx := handle.Provide()(t.Context())
			b := newFlowBuilder(ctx, execution.NewFlowExecutionWithContainer(copy))
			ref, _ = handle.Use(b)
			assert.Equal(t, byte(101), *ref)
		})
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
		)

		ctx := h.Provide()(t.Context())

		inner := executiontype.NewInMemoryContainer()
		err := inner.StoreDurable(durableKey, []byte{byte(42)})
		assert.NoError(t, err)

		counting := &storeCountingContainer{inner: inner}
		b := newFlowBuilder(ctx, execution.NewFlowExecutionWithContainer(counting))

		ref, persist := h.Use(b)
		assert.Equal(t, byte(42), *ref)

		t.Run("no change is a no-op", func(t *testing.T) {
			didChange := persist()
			assert.False(t, didChange)
			assert.Equal(t, int32(0), counting.storeCalls.Load())

			value, ok, err := inner.LoadDurable(durableKey)
			assert.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, []byte{byte(42)}, value)
		})

		t.Run("change stores once, then becomes a no-op", func(t *testing.T) {
			*ref = byte(43)

			didChange := persist()
			assert.True(t, didChange)
			assert.Equal(t, int32(1), counting.storeCalls.Load())

			didChange = persist()
			assert.False(t, didChange)
			assert.Equal(t, int32(1), counting.storeCalls.Load())

			value, ok, err := inner.LoadDurable(durableKey)
			assert.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, []byte{byte(43)}, value)
		})
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
		)

		ctx := callsHandle.Provide()(t.Context())
		c := executiontype.NewInMemoryContainer()
		b := newFlowBuilder(ctx, execution.NewFlowExecutionWithContainer(c))

		ref, persist := callsHandle.Use(b)
		assert.Equal(t, 7, *ref)
		assert.Equal(t, 1, constructorCalls)
		assert.Equal(t, 0, unmarshalCalls)
		assert.Equal(t, 0, marshalCalls)

		persist()
		assert.Equal(t, 1, marshalCalls)

		ref, _ = callsHandle.Use(b)
		assert.Equal(t, 7, *ref)
		assert.Equal(t, 1, constructorCalls)
		assert.Equal(t, 1, unmarshalCalls)
		assert.Equal(t, 1, marshalCalls)
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
		)

		ctx := errHandle.Provide()(t.Context())
		c := executiontype.NewInMemoryContainer()
		err := c.StoreDurable("unmarshalErrHandle", []byte{123})
		assert.NoError(t, err)
		b := newFlowBuilder(ctx, execution.NewFlowExecutionWithContainer(c))

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
		)

		ctx := errHandle.Provide()(t.Context())
		c := executiontype.NewInMemoryContainer()
		b := newFlowBuilder(ctx, execution.NewFlowExecutionWithContainer(c))

		_, persist := errHandle.Use(b)
		testutil.PanicsWithErrorIs(t, expectedErr, func() { persist() })
		assert.Equal(t, 1, marshalCalls)

		_, ok, err := c.LoadDurable("marshalErrHandle")
		assert.NoError(t, err)
		assert.False(t, ok)
	})
}
