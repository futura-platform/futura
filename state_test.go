package futura_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/internal/utils/containertest"
	"github.com/stretchr/testify/assert"
)

// crashingContainer simulates the process dying partway through an execution by panicking on
// the nth durable write, so that a fresh execution can be resumed over whatever was committed.
type crashingContainer struct {
	*executiontype.InMemoryContainer
	writes  int
	crashAt int
}

var errSimulatedCrash = errors.New("simulated crash")

func (c *crashingContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	return c.InMemoryContainer.Transact(ctx, func(ctx context.Context, _ executiontype.Container) error { return fn(ctx, c) })
}

func (c *crashingContainer) StoreDurable(key string, value []byte) error {
	c.writes++
	if c.writes == c.crashAt {
		panic(errSimulatedCrash)
	}
	return c.InMemoryContainer.StoreDurable(key, value)
}

// executeUntilCrash runs the flow over c and reports whether it ended in the simulated crash.
// Any other outcome is a test failure, since the crash is the only way the execution is expected to end.
func executeUntilCrash(t *testing.T, c *crashingContainer, flowFn futura.FlowFn[struct{}, int]) {
	t.Helper()
	_, err := futura.NewFlowFromContainer[struct{}, int](c).Execute(t.Context(), flowFn, struct{}{})
	assert.ErrorIs(t, err, errSimulatedCrash)
}

func TestState(t *testing.T) {
	t.Run("no initial value implies the default to be the type's zero value", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State[int](b)
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, r)
	})
	t.Run("an initial value can be provided", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("multiple initial values will cause a panic", func(t *testing.T) {
		assert.Panics(t, func() {
			futura.State(futura.FlowBuilder{}, 1, 2)
		})
	})
	t.Run("setState does not trigger a replay if the new value is the same as the current value", func(t *testing.T) {
		replays := 0
		futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			state := futura.State(b, 1)
			state.Set(1)
			return state.V(), nil
		}, struct{}{})
		assert.Equal(t, 1, replays)
	})
	t.Run("setState updates the state and immediately triggers a replay for a new value", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			state.Set(2)
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)
	})
	t.Run("setState does not evict unseen cached states within the same replay", func(t *testing.T) {
		replays := 0
		unseenAtStateEvals := 0
		eventOrder := []string{}
		futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			state := futura.State(b, false)
			if replays == 2 {
				eventOrder = append(eventOrder, "stateSet")
				state.Set(true)
			}
			err := futura.Action(b, func(ctx context.Context) error {
				unseenAtStateEvals++
				eventOrder = append(eventOrder, "unseenAtStateEval")
				return nil
			})
			if err != nil {
				return 0, err
			}
			return 1, futura.Action(b, func(ctx context.Context) error {
				if replays < 2 {
					return errors.New("keep replaying")
				}
				return nil
			})
		}, struct{}{})
		assert.Equal(t, 3, replays)
		assert.Equal(t, 1, unseenAtStateEvals)
		assert.Equal(t, []string{"unseenAtStateEval", "stateSet"}, eventOrder)
	})
	t.Run("state is restored after execution is restarted from the same container", func(t *testing.T) {
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			if state.V() == 1 {
				state.Set(2)
			}
			return state.V(), nil
		}

		originalContainer := executiontype.NewInMemoryContainer()

		r, err := futura.NewFlowFromContainer[struct{}, int](originalContainer).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)

		// Simulate handing execution state to another machine/process by creating
		// a new flow instance over the persisted execution container.
		r, err = futura.NewFlowFromContainer[struct{}, int](originalContainer).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)
	})
	t.Run("a branch change from setState survives an interrupted replay and a restart from the same container", func(t *testing.T) {
		flowFn := func(simulateOutage context.CancelFunc) futura.FlowFn[struct{}, int] {
			return func(b futura.FlowBuilder, _ struct{}) (int, error) {
				state := futura.State(b, false)
				if !state.V() {
					if err := futura.Action(b, func(ctx context.Context) error { return nil }); err != nil {
						return 0, err
					}
					state.Set(true)
					// The state change should be durably persisted at this point
					simulateOutage()
					return 0, nil
				}
				return 1, futura.Action(b, func(ctx context.Context) error { return nil })
			}
		}

		originalContainer := executiontype.NewInMemoryContainer()

		ctx, cancel := context.WithCancel(t.Context())
		_, err := futura.NewFlowFromContainer[struct{}, int](originalContainer).Execute(ctx, flowFn(cancel), struct{}{})
		assert.ErrorIs(t, err, context.Canceled)

		// move the flow into a new execution context
		r, err := futura.NewFlowFromContainer[struct{}, int](originalContainer).Execute(t.Context(), flowFn(func() {}), struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("a new branch whose first step fails can be retried after the relaxed replay settles", func(t *testing.T) {
		newBranchCalls := 0
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, false)
			if !state.V() {
				// The old branch must be longer than the point where the new branch
				// fails, so that a stale call order entry is left behind.
				if err := futura.Action(b, func(ctx context.Context) error { return nil }); err != nil {
					return 0, err
				}
				if err := futura.Action(b, func(ctx context.Context) error { return nil }); err != nil {
					return 0, err
				}
				state.Set(true)
				return 0, nil
			}
			err := futura.Action(b, func(ctx context.Context) error {
				newBranchCalls++
				if newBranchCalls == 1 {
					return errors.New("transient failure on the first attempt of the new branch")
				}
				return nil
			})
			if err != nil {
				return 0, err
			}
			return 1, futura.Action(b, func(ctx context.Context) error { return nil })
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
		assert.Equal(t, 2, newBranchCalls)
	})
	t.Run("state values can be used as branch conditions", func(t *testing.T) {
		fn1Calls := 0
		fn1 := func(_ context.Context, _ struct{}) (int, error) {
			fn1Calls++
			return fn1Calls, nil
		}
		fn2Calls := 0
		failsTwice := func(_ context.Context, _ struct{}) (int, error) {
			fn2Calls++
			if fn2Calls <= 2 {
				return 0, errors.New("expected error")
			}
			return fn2Calls, nil
		}
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 0)

			var r1, r2 int
			var err error
			if state.V() != 1 {
				r1, err = fn1(b, struct{}{})
				if err != nil {
					return 0, err
				}
			}
			r2, err = failsTwice(b, struct{}{})
			if err != nil {
				state.Set(state.V() + 1)
				return 0, err
			}
			return r1 + r2, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, fn1Calls)
		assert.Equal(t, 3, fn2Calls)
		assert.Equal(t, 5, r)
	})
	t.Run("the context can be cancelled before a states initial value is seeded", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		reachedAfterSeed := false
		r, err := futura.NewFlow[struct{}, int]().Execute(ctx, func(b futura.FlowBuilder, _ struct{}) (int, error) {
			cancel()
			state := futura.State(b, 1) // the seed is a step: the cancelled replay terminates here
			reachedAfterSeed = true
			return state.V(), nil
		}, struct{}{})
		assert.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, futura.ErrFlowPanic)
		assert.False(t, reachedAfterSeed)
		assert.Equal(t, 0, r)
	})
	t.Run("setState is callable from any goroutine", func(t *testing.T) {
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			state := futura.State(b, 1)
			var wg sync.WaitGroup
			wg.Go(func() {
				state.Set(2)
			})
			wg.Wait()
			return state.V(), nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 2, r)
	})
	t.Run("setState keys on the entire user callstack, not just the top frame", func(t *testing.T) {
		helperFn := func(b futura.FlowBuilder) futura.StateContainer[int] {
			return futura.State(b, 0)
		}
		middleFn := func(b futura.FlowBuilder) (int, error) {
			s1 := helperFn(b)
			s2 := helperFn(b)
			s1.Set(1)
			assert.Equal(t, 1, s1.V())
			assert.Equal(t, 0, s2.V())
			return s1.V() + s2.V(), nil
		}
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			return middleFn(b)
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("consecutive state changes in one replay are committed atomically", func(t *testing.T) {
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			open := futura.State(b, false)
			generation := futura.State(b, 0)
			if !open.V() {
				open.Set(true)
				generation.Set(generation.V() + 1)
				return 0, nil
			}
			return generation.V(), nil
		}

		writes := func() int {
			c := &crashingContainer{InMemoryContainer: executiontype.NewInMemoryContainer()}
			futura.NewFlowFromContainer[struct{}, int](c).Execute(t.Context(), flowFn, struct{}{})
			return c.writes
		}()

		for crashAt := 1; crashAt <= writes; crashAt++ {
			t.Run(fmt.Sprintf("crash after write %d", crashAt), func(t *testing.T) {
				c := &crashingContainer{InMemoryContainer: executiontype.NewInMemoryContainer(), crashAt: crashAt}
				executeUntilCrash(t, c, flowFn)

				c.crashAt = 0
				r, err := futura.NewFlowFromContainer[struct{}, int](c).Execute(t.Context(), flowFn, struct{}{})
				assert.NoError(t, err)
				assert.Equal(t, 1, r)
			})
		}
	})
	t.Run("states can be read and set concurrently from different goroutines", func(t *testing.T) {
		// Every State in a flow shares one value map, so a Set from a goroutine must not
		// race a V() of a different State on the main goroutine, nor the loop's marshal
		// of the map when it commits. Run under -race.
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			a := futura.State(b, 0)
			c := futura.State(b, 0)
			if a.V() > 0 {
				return a.V() + c.V(), nil
			}
			return 0, futura.Action(b, func(ctx context.Context) error {
				var wg sync.WaitGroup
				wg.Go(func() { a.Set(1) })
				for range 1000 {
					_ = c.V()
				}
				wg.Wait()
				return nil
			})
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("a state change ends the replay at the next step, so nothing after it can act on the dead replay", func(t *testing.T) {
		t.Run("a state declared after the change cannot leak an unseeded value into another state", func(t *testing.T) {
			r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
				trigger := futura.State(b, false)
				total := futura.State(b, -1)
				if !trigger.V() {
					trigger.Set(true)
					// the seed of this state is a step: the replay terminates here
					source := futura.State(b, 42)
					total.Set(source.V())
					return 0, nil
				}
				return total.V(), nil
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, -1, r)
		})
		t.Run("a step after the change cannot be observed as a failure", func(t *testing.T) {
			r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
				ready := futura.State(b, false)
				failures := futura.State(b, 0)
				if !ready.V() {
					ready.Set(true)
				}
				err := futura.Action(b, func(ctx context.Context) error { return ctx.Err() })
				if err != nil {
					failures.Set(failures.V() + 1)
					return 0, err
				}
				return failures.V(), nil
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, 0, r)
		})
		t.Run("pure code after the change still runs, so consecutive state changes stay atomic", func(t *testing.T) {
			r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
				open := futura.State(b, false)
				generation := futura.State(b, 0)
				if !open.V() {
					open.Set(true)
					generation.Set(generation.V() + 1)
					return 0, nil
				}
				return generation.V(), nil
			}, struct{}{})
			assert.NoError(t, err)
			assert.Equal(t, 1, r)
		})
	})
	t.Run("a state container from a previous replay restarts the replay that is currently running", func(t *testing.T) {
		// Set is callable from anywhere, including through a container that was created in an
		// earlier replay and held across a restart. It must restart the replay running now, not
		// the dead one it was created in, or the running replay would read the changed value
		// and take a new branch under strict checking.
		var held futura.StateContainer[bool]
		replays := 0
		r, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			s := futura.State(b, false)
			switch replays {
			case 1:
				held = s
				return 0, futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			case 2:
				var wg sync.WaitGroup
				wg.Go(func() { held.Set(true) })
				wg.Wait()
			}
			if s.V() {
				return 1, futura.Action(b, func(ctx context.Context) error { return nil })
			}
			return 0, futura.Action(b, func(ctx context.Context) error { return nil })
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("a state change is durable when the container retries its transactions", func(t *testing.T) {
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			s := futura.State(b, 0)
			if s.V() == 0 {
				s.Set(1)
				return 0, nil
			}
			return s.V(), nil
		}

		c := executiontype.NewInMemoryContainer()
		r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewRetrying(c, 3)).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)

		// a fresh process over the committed state must see the change
		r, err = futura.NewFlowFromContainer[struct{}, int](c).Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("a state change with no replay running is applied by the next replay to start", func(t *testing.T) {
		var held futura.StateContainer[int]
		flowFn := func(b futura.FlowBuilder, _ struct{}) (int, error) {
			s := futura.State(b, 0)
			held = s
			return s.V(), nil
		}
		f := futura.NewFlow[struct{}, int]()
		r, err := f.Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 0, r)

		// nothing is running: the change waits for the next execution
		assert.NotPanics(t, func() { held.Set(7) })

		r, err = f.Execute(t.Context(), flowFn, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 7, r)
	})
}
