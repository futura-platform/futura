package futura_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

// keyStoreCountingContainer counts the stores made to one durable key.
type keyStoreCountingContainer struct {
	*executiontype.InMemoryContainer
	key    string
	stores atomic.Int32
}

type keyStoreCountingTx struct {
	executiontype.Container
	c *keyStoreCountingContainer
}

func (tx *keyStoreCountingTx) StoreDurable(key string, value []byte) error {
	if key == tx.c.key {
		tx.c.stores.Add(1)
	}
	return tx.Container.StoreDurable(key, value)
}

func (c *keyStoreCountingContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	return c.InMemoryContainer.Transact(ctx, func(ctx context.Context, tx executiontype.Container) error {
		return fn(ctx, &keyStoreCountingTx{Container: tx, c: c})
	})
}

// A handle is durable state that steps mutate as a side effect. Its changes are committed with the
// memo of the step that made them: the memo is the record that the step's effects happened, so it is
// never durable without them. Nothing has to be called to make that happen.
func TestDurableHandle_ChangesAreCommittedWithTheStepsMemo(t *testing.T) {
	t.Run("a change made inside a step survives a crash after any commit", func(t *testing.T) {
		h := futura.NewPlainDurableHandle("flush-in-step", func() *int { v := 0; return &v })
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			b = h.Provide(b)
			ref := h.Use(b)
			if err := futura.Action(b, func(ctx context.Context) error {
				if *ref == 0 {
					*ref = 42
				}
				return nil
			}); err != nil {
				return 0, err
			}
			return *ref, nil
		}

		c := &crashingContainer{InMemoryContainer: executiontype.NewInMemoryContainer()}
		r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 42, r)
		commits := c.commits

		for crashAfter := 1; crashAfter <= commits; crashAfter++ {
			t.Run(fmt.Sprintf("crash after commit %d", crashAfter), func(t *testing.T) {
				c := &crashingContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), crashAfterCommit: crashAfter}
				_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
				assert.ErrorIs(t, err, errSimulatedCrash)

				c.crashAfterCommit = 0
				r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
				assert.NoError(t, err)
				assert.Equal(t, 42, r, "the change made by the memoized step was lost")
			})
		}
	})
	t.Run("a change made by a step that then fails is committed with the failure", func(t *testing.T) {
		// the shape of a failure counter kept in a wrapper: the step fails, the counter moves, and the
		// count must be durable before the next replay retries
		h := futura.NewPlainDurableHandle("flush-on-failure", func() *int { v := 0; return &v })
		attempts := 0
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			b = h.Provide(b)
			ref := h.Use(b)
			err := futura.Action(b, func(ctx context.Context) error {
				attempts++
				*ref++
				if *ref < 3 {
					return errors.New("not yet")
				}
				return nil
			})
			return *ref, err
		}
		c := &crashingContainer{InMemoryContainer: executiontype.NewInMemoryContainer()}
		r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 3, r)
		assert.Equal(t, 3, attempts)
		commits := c.commits

		for crashAfter := 1; crashAfter <= commits; crashAfter++ {
			t.Run(fmt.Sprintf("crash after commit %d", crashAfter), func(t *testing.T) {
				attempts = 0
				c := &crashingContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), crashAfterCommit: crashAfter}
				_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
				assert.ErrorIs(t, err, errSimulatedCrash)
				before := attempts

				c.crashAfterCommit = 0
				r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), flowFn, struct{}{})
				assert.NoError(t, err)
				assert.Equal(t, 3, r)
				// every recorded failure stays counted: at most the attempt in flight at the crash repeats
				assert.LessOrEqual(t, attempts, 3+1, "a recorded failure was forgotten and retried: %d before the crash, %d after", before, attempts-before)
			})
		}
	})
	t.Run("a change made inside a step is visible to a later step, and durable, with no call in between", func(t *testing.T) {
		h := futura.NewPlainDurableHandle("flush-visible", func() *int { v := 0; return &v })
		c := executiontype.NewInMemoryContainer()
		var seen int
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			b = h.Provide(b)
			ref := h.Use(b)
			if err := futura.Action(b, func(ctx context.Context) error { *ref = 5; return nil }); err != nil {
				return 0, err
			}
			return 0, futura.Action(b, func(ctx context.Context) error { seen = *ref; return nil })
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 5, seen)
		stored, ok, err := c.LoadDurable(execution.GenericDurableKey("flush-visible"))
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.NotEmpty(t, stored)
	})
	t.Run("a value that cannot be marshaled fails the flow", func(t *testing.T) {
		marshalErr := errors.New("marshal failed")
		h := futura.NewDurableHandle[int]("flush-marshal-error",
			func() *int { v := 0; return &v },
			func(input []byte) (*int, error) { v := int(input[0]); return &v, nil },
			func(*int) ([]byte, error) { return nil, marshalErr },
			nil,
		)
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			b = h.Provide(b)
			ref := h.Use(b)
			return 0, futura.Action(b, func(ctx context.Context) error { *ref = 1; return nil })
		}, struct{}{}, fopt.WithMaxFailures(2))
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, marshalErr)
	})
	t.Run("an unchanged value is not stored again at every boundary", func(t *testing.T) {
		h := futura.NewPlainDurableHandle("flush-unchanged", func() *int { v := 0; return &v })
		counting := &keyStoreCountingContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), key: execution.GenericDurableKey("flush-unchanged")}
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(counting)).Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			b = h.Provide(b)
			ref := h.Use(b)
			if err := futura.Action(b, func(ctx context.Context) error { *ref = 1; return nil }); err != nil {
				return 0, err
			}
			for i := range 5 {
				if err := futura.Action(b.WithKey(fmt.Sprint(i)), func(ctx context.Context) error { return nil }); err != nil {
					return 0, err
				}
			}
			return *ref, nil
		}, struct{}{})
		assert.NoError(t, err)
		// the value is stored exactly once: at the boundary after it changed
		assert.Equal(t, int32(1), counting.stores.Load())
	})
}
