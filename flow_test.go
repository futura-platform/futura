package futura_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestFlow(t *testing.T) {
	t.Run("basic e2e test", func(t *testing.T) {
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		r, err := f.Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			return "result", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "result", r)
	})
	t.Run("do not call Flow from within a flow", func(t *testing.T) {
		outerExec := execution.NewFlowExecutionWithContainer(containertest.NewInMemory())
		stop, _ := outerExec.TryStartRun()
		defer stop()
		ctx := execution.WithFlow(t.Context(), outerExec)
		f1 := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		f2 := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		_, err := f1.Execute(ctx, func(b futura.FlowBuilder, _ *any) (string, error) {
			f2.Execute(ctx, func(b futura.FlowBuilder, _ *any) (string, error) {
				return "never reached 1", nil
			}, nil)
			return "never reached 2", nil
		}, nil)
		assert.ErrorIs(t, err, futura.ErrTopLevelFlowConflict)
	})
	t.Run("a flow context from an execution that is not running is reported as a flow panic", func(t *testing.T) {
		// the conflict check asserts on the context it is given; that assertion must be recovered like any other
		stale := execution.WithFlow(t.Context(), execution.NewFlowExecutionWithContainer(containertest.NewInMemory()))
		var err error
		assert.NotPanics(t, func() {
			_, err = futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(stale, func(b futura.FlowBuilder, _ *any) (string, error) {
				return "never reached", nil
			}, nil)
		})
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
		assert.ErrorIs(t, err, execution.ErrFlowExecutionNotRunning)
	})
	t.Run("do not execute a flow more than once concurrently", func(t *testing.T) {
		fnEntered := make(chan struct{})
		goroutine2Finished := make(chan struct{})
		fn := func(b futura.FlowBuilder, _ *any) (string, error) {
			close(fnEntered)
			<-goroutine2Finished
			return "result", nil
		}
		f := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory())
		go func() {
			<-fnEntered
			defer close(goroutine2Finished)
			_, err := f.Execute(t.Context(), fn, nil)
			assert.ErrorIs(t, err, futura.ErrAlreadyRunning)
		}()
		r, err := f.Execute(t.Context(), fn, nil)
		assert.NoError(t, err)
		assert.Equal(t, "result", r)
	})
	t.Run("Flow recovers from panics", func(t *testing.T) {
		var expectedErr = errors.New("expected panic")
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			panic(expectedErr)
		}, nil)
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
		assert.ErrorIs(t, err, expectedErr)
	})
	t.Run("Flow recovers from panics with non-error values", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			panic("not an error type")
		}, nil)
		assert.ErrorIs(t, err, futura.ErrFlowPanic)
		assert.Contains(t, err.Error(), "not an error type")
	})

	fn1 := func(ctx context.Context, args *any) (string, error) {
		return "fn1", errors.New("expected error")
	}
	fn2 := func(ctx context.Context, args *any) (string, error) {
		return "fn2", nil
	}
	checkMultipleMomentFunctions := func(t *testing.T, onUseFn1 func(futura.FlowBuilder) futura.FlowBuilder, onUseFn2 func(futura.FlowBuilder) futura.FlowBuilder) (string, error) {
		replayCount := 0
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			var vfn futura.ComparableMomentFn[*any, string]
			if replayCount == 0 {
				vfn = fn1
				if onUseFn1 != nil {
					b = onUseFn1(b)
				}
			} else {
				vfn = fn2
				if onUseFn2 != nil {
					b = onUseFn2(b)
				}
			}
			replayCount++
			return futura.Step(b, vfn, nil)
		}, nil)
		assert.Equal(t, 2, replayCount)
		return r, err
	}
	t.Run("A single keyless moment identity should only ever be used with a single moment function", func(t *testing.T) {
		_, err := checkMultipleMomentFunctions(t, nil, nil)
		assert.ErrorIs(t, err, ftrerrors.ErrInconsistentState)
		var fnChange moment.MomentFnChangeError
		assert.ErrorAs(t, err, &fnChange)
		assert.Equal(t, 0, fnChange.Index)
		assert.Equal(t, runtime.FuncForPC(reflect.ValueOf(fn1).Pointer()).Name(), fnChange.OldMomentFnName)
		assert.Equal(t, runtime.FuncForPC(reflect.ValueOf(fn2).Pointer()).Name(), fnChange.NewMomentFnName)
		// the identity's callpath spans from the flow fn's Step call up through futura's own
		// Execute wrapper, so pin it by its user callsite rather than reconstructing every frame.
		assert.Contains(t, fnChange.Identity.Callpath().V(), moment.Callsite{File: "github.com/futura-platform/futura_test/flow_test.go", Line: 114})
	})
	t.Run("A single keyed moment identity should be able to be used with multiple moment functions", func(t *testing.T) {
		r, err := checkMultipleMomentFunctions(t, func(b futura.FlowBuilder) futura.FlowBuilder {
			return b.WithKey("1")
		}, func(b futura.FlowBuilder) futura.FlowBuilder {
			return b.WithKey("2")
		})
		assert.NoError(t, err)
		assert.Equal(t, "fn2", r)
	})
	t.Run("layered keys do not collide with each other or with a single key", func(t *testing.T) {
		calls := 0
		r, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
			step := func(b futura.FlowBuilder) error {
				return futura.Action(b, func(ctx context.Context) error { calls++; return nil })
			}
			for _, kb := range []futura.FlowBuilder{
				b.WithKey("a-b").WithKey("c"),
				b.WithKey("a").WithKey("b-c"),
				b.WithKey("a-b-c"),
			} {
				if err := step(kb); err != nil {
					return "", err
				}
			}
			return "done", nil
		}, nil)
		assert.NoError(t, err)
		assert.Equal(t, "done", r)
		assert.Equal(t, 3, calls)
	})
	t.Run("A single keyed moment identity should be able to be used with a single moment function, and have memoization keyed by the identity key", func(t *testing.T) {
		expectedExecCount := 10
		execCount := 0
		fn := func(ctx context.Context, _ *struct{}) error {
			execCount++
			return nil
		}
		_, err := futura.NewFlowFromContainer[*any, string](containertest.NewInMemory()).
			Execute(t.Context(), func(b futura.FlowBuilder, _ *any) (string, error) {
				for i := range expectedExecCount {
					b = b.WithKey(strconv.Itoa(i))
					err := futura.Effect(b, fn, nil)
					assert.NoError(t, err)
				}
				return "", nil
			}, nil)
		assert.NoError(t, err)
		assert.Equal(t, expectedExecCount, execCount)
	})
	t.Run("Flow can be persisted in an execution container, so that the execution state resumes from where it left off", func(t *testing.T) {
		step1Called := make(chan struct{})
		defer close(step1Called)

		step1Calls := 0
		step2Calls := 0

		step1 := func(_ context.Context, _ struct{}) (string, error) {
			step1Calls++
			step1Called <- struct{}{}
			return "step1", nil
		}
		step2 := func(_ context.Context, _ struct{}) (string, error) {
			step2Calls++
			return "step2", nil
		}
		flowFn := func(b futura.FlowBuilder, _ *any) (string, error) {
			r1, err := futura.Step(b, step1, struct{}{})
			if err != nil {
				return "", err
			}
			// give the context time to be cancelled
			time.Sleep(time.Millisecond * 100)
			r2, err := futura.Step(b, step2, struct{}{})
			if err != nil {
				return "", err
			}
			return r1 + r2, nil
		}

		container := executiontype.NewInMemoryContainer()

		// perform the first execution
		f1 := futura.NewFlowFromContainer[*any, string](containertest.NewStrict(container))

		firstExecutionContext, cancelFirstExecution := context.WithCancel(t.Context())
		defer cancelFirstExecution()

		go func() {
			<-step1Called
			cancelFirstExecution()
		}()

		_, err := f1.Execute(firstExecutionContext, flowFn, nil)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 1, step1Calls)
		assert.Equal(t, 0, step2Calls)

		// simulate a context switch
		f2 := futura.NewFlowFromContainer[*any, string](containertest.NewStrict(container))

		// resume the execution
		_, err = f2.Execute(t.Context(), flowFn, nil)
		assert.NoError(t, err)
		assert.Equal(t, 1, step1Calls)
		assert.Equal(t, 1, step2Calls)
	})
}
