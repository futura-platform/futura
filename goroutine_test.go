package futura_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/fopt"
	"github.com/futura-platform/futura/ftype"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/internal/step"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/petermattis/goid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBindToGoroutine(t *testing.T) {
	t.Run("BindToGoroutine panics outside of a step function", func(t *testing.T) {
		assert.Panics(t, func() {
			futura.BindToGoroutine(context.Background())
		})
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				_, cancel := futura.BindToGoroutine(context.Background())
				defer cancel()
				return "", nil
			}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
	})
	t.Run("BindToGoroutine passes AssertBoundGoroutine", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return "", futura.Action(b, func(ctx context.Context) error {
					_, cancel := futura.BindToGoroutine(ctx)
					defer cancel()
					assert.NoError(t, goroutinebind.AssertBoundGoroutine(ctx))
					return nil
				})
			}, struct{}{})
		assert.NoError(t, err)
	})
	t.Run("a step that spawns a goroutine that exits in time succeeds", func(t *testing.T) {
		var ranInGoroutine atomic.Bool
		var assertBoundErr error
		var spawnedRoutineID, observedRoutineID int64

		result, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					var wg sync.WaitGroup
					wg.Go(func() {
						spawnedRoutineID = goid.Get()
						boundCtx, cancel := futura.BindToGoroutine(ctx)
						defer cancel()
						assertBoundErr = goroutinebind.AssertBoundGoroutine(boundCtx)
						observedRoutineID = goid.Get()
						ranInGoroutine.Store(true)
					})
					wg.Wait()
					return "ok", nil
				})
			}, struct{}{})

		assert.NoError(t, err)
		assert.Equal(t, "ok", result)
		assert.True(t, ranInGoroutine.Load(), "expected the spawned goroutine to run")
		assert.NoError(t, assertBoundErr, "the returned context should be bound to the spawning goroutine")
		assert.Equal(t, spawnedRoutineID, observedRoutineID,
			"sanity check: the bound goroutine should be the same goroutine that called BindToGoroutine")
	})

	t.Run("a step that spawns a goroutine that does not exit in time fails", func(t *testing.T) {
		// Block the goroutine until the test finishes so we can prove the step
		// returned while the goroutine was still active. The deferred close lets
		// the goroutine exit cleanly so we don't leak it across tests.
		keepGoroutineRunning := make(chan struct{})
		defer close(keepGoroutineRunning)

		goroutineStarted := make(chan struct{})

		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					go func() {
						_, cancel := futura.BindToGoroutine(ctx)
						defer cancel()
						close(goroutineStarted)
						<-keepGoroutineRunning
					}()
					<-goroutineStarted
					return "ok", nil
				})
			}, struct{}{})

		require.Error(t, err)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic,
			"the step should panic when it returns while goroutines are still bound")
		assert.ErrorIs(t, err, step.ErrGoroutinesNotExited)
	})

	t.Run("calling BindToGoroutine twice from the same goroutine panics", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[struct{}, struct{}](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (struct{}, error) {
				_, err := futura.Source(b, func(ctx context.Context) (struct{}, error) {
					_, cancel := futura.BindToGoroutine(ctx)
					defer cancel()
					_, _ = futura.BindToGoroutine(ctx)
					return struct{}{}, nil
				})
				return struct{}{}, err
			}, struct{}{})

		require.Error(t, err)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, futura.ErrGoroutineAlreadyBound)
	})

	t.Run("multiple concurrent goroutines must all exit before the step returns", func(t *testing.T) {
		const n = 10
		var counter atomic.Int64

		result, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (int, error) {
				return futura.Source(b, func(ctx context.Context) (int, error) {
					var wg sync.WaitGroup
					for range n {
						wg.Go(func() {
							_, cancel := futura.BindToGoroutine(ctx)
							defer cancel()
							counter.Add(1)
						})
					}
					wg.Wait()
					return int(counter.Load()), nil
				})
			}, struct{}{})

		assert.NoError(t, err)
		assert.Equal(t, n, result)
	})

	t.Run("a step that leaks a goroutine records nothing, since the goroutine may still be writing", func(t *testing.T) {
		type pair struct{ A, B int }
		recovering := fopt.WithStepWrapper(func(ctx context.Context, fnLabel string, _ any, _ []runtime.Frame, call func() (any, error)) (errOverride error) {
			defer func() {
				if r := recover(); r != nil {
					errOverride = fmt.Errorf("step %s panicked: %w", fnLabel, ftrerrors.PanicError(r))
				}
			}()
			_, err := call()
			return err
		})
		for name, tc := range map[string]struct {
			exit func() error
			opts []ftype.FlowLoopOption
		}{
			"the step returns": {exit: func() error { return nil }},
			"the step panics":  {exit: func() error { panic("boom") }},
			"the step returns under a recovering wrapper": {exit: func() error { return nil }, opts: []ftype.FlowLoopOption{recovering}},
		} {
			t.Run(name, func(t *testing.T) {
				h := futura.NewPlainDurableHandle("leakedWriter", func() *pair { return &pair{} })
				c := executiontype.NewInMemoryContainer()
				stop := make(chan struct{})
				defer close(stop)
				bound := make(chan struct{})
				_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(),
					func(b futura.FlowBuilder, _ struct{}) (int, error) {
						b = h.Provide(b)
						ref := h.Use(b)
						return 0, futura.Action(b, func(ctx context.Context) error {
							go func() {
								_, done := futura.BindToGoroutine(ctx)
								defer done()
								close(bound)
								for {
									select {
									case <-stop:
										return
									default:
										ref.A++
										ref.B++
									}
								}
							}()
							<-bound
							return tc.exit()
						})
					}, struct{}{}, tc.opts...)
				assert.ErrorIs(t, err, step.ErrGoroutinesNotExited)
				_, ok, _ := c.LoadDurable(execution.GenericDurableKey("leakedWriter"))
				assert.False(t, ok, "the handle was flushed while the leaked goroutine was writing it")
				assert.Equal(t, 0, c.CallOrderLength(), "the step was recorded")
			})
		}
	})
	t.Run("a goroutine that has bound but not yet finished work fails the step even when sleeping briefly", func(t *testing.T) {
		// This documents the boundary: the active-goroutines check happens
		// synchronously after the step's main fn returns. Once the goroutine
		// has bound, even a brief outstanding sleep causes the check to fail.
		bound := make(chan struct{})
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					go func() {
						_, cancel := futura.BindToGoroutine(ctx)
						defer cancel()
						close(bound)
						time.Sleep(50 * time.Millisecond)
					}()
					<-bound
					return "ok", nil
				})
			}, struct{}{})

		require.Error(t, err)
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, step.ErrGoroutinesNotExited)
	})

	t.Run("the same goroutine can be bound again after cancel", func(t *testing.T) {
		// Cancel removes the goroutine id from the active set, so a subsequent
		// bind from the same goroutine should succeed without panicking.
		result, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					var wg sync.WaitGroup
					wg.Go(func() {
						_, cancel1 := futura.BindToGoroutine(ctx)
						cancel1()
						_, cancel2 := futura.BindToGoroutine(ctx)
						cancel2()
					})
					wg.Wait()
					return "ok", nil
				})
			}, struct{}{})

		assert.NoError(t, err)
		assert.Equal(t, "ok", result)
	})
}
