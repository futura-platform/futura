package futura_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
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
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:noinline
func goroutineThatPanics() { panic("goroutine boom") }

func TestGo(t *testing.T) {
	t.Run("Go panics outside of a step function", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, step.ErrGoOutsideOfAStep, func() {
			futura.Go(context.Background(), func(ctx context.Context) {})
		})
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				futura.Go(b, func(ctx context.Context) {})
				return "", nil
			}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, step.ErrGoOutsideOfAStep)
	})
	t.Run("waiting on a goroutine that was never started returns", func(t *testing.T) {
		var g futura.Goroutine
		g.Wait()
	})
	t.Run("the goroutine's context is bound to it", func(t *testing.T) {
		var assertBoundErr error
		result, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					assertBoundErr = errors.New("the goroutine did not run")
					futura.Go(ctx, func(ctx context.Context) {
						assertBoundErr = goroutinebind.AssertBoundGoroutine(ctx)
					}).Wait()
					return "ok", nil
				})
			}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, "ok", result)
		assert.NoError(t, assertBoundErr)
	})
	t.Run("a step that returns under a running goroutine fails", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		for name, exit := range map[string]func(ctx context.Context) (string, error){
			"before the goroutine starts": func(ctx context.Context) (string, error) { return "ok", nil },
			"while the goroutine runs": func(ctx context.Context) (string, error) {
				started := make(chan struct{})
				futura.Go(ctx, func(ctx context.Context) { close(started); <-release })
				<-started
				return "ok", nil
			},
			"with the step's own error": func(ctx context.Context) (string, error) {
				futura.Go(ctx, func(ctx context.Context) { <-release })
				return "", errors.New("early return root cause")
			},
			"with the step's own panic": func(ctx context.Context) (string, error) {
				futura.Go(ctx, func(ctx context.Context) { <-release })
				panic("step panic root cause")
			},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
					func(b futura.FlowBuilder, _ struct{}) (string, error) {
						return futura.Source(b, func(ctx context.Context) (string, error) {
							if name == "before the goroutine starts" {
								futura.Go(ctx, func(ctx context.Context) { <-release })
							}
							return exit(ctx)
						})
					}, struct{}{})
				assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
				assert.ErrorIs(t, err, step.ErrGoroutinesNotExited)
				if strings.Contains(name, "own") {
					assert.ErrorContains(t, err, "root cause", "the step's own exit is reported with the leak")
				}
			})
		}
	})
	t.Run("waiting on the goroutine is enough", func(t *testing.T) {
		// Wait returns once the goroutine is unregistered, so a step that waits never sees it as running
		const n = 10
		spurious := 0
		for range 200 {
			var counter atomic.Int64
			result, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(),
				func(b futura.FlowBuilder, _ struct{}) (int, error) {
					return futura.Source(b, func(ctx context.Context) (int, error) {
						var goroutines []futura.Goroutine
						for range n {
							goroutines = append(goroutines, futura.Go(ctx, func(ctx context.Context) {
								defer time.Sleep(time.Microsecond) // work after any signal fn might give
								counter.Add(1)
							}))
						}
						for _, g := range goroutines {
							g.Wait()
						}
						return int(counter.Load()), nil
					})
				}, struct{}{})
			if errors.Is(err, step.ErrGoroutinesNotExited) {
				spurious++
				continue
			}
			require.NoError(t, err)
			require.Equal(t, n, result)
		}
		assert.Equal(t, 0, spurious)
	})
	t.Run("a goroutine that panics crashes the step", func(t *testing.T) {
		// a goroutine returns its errors to the step, like any Go code; a panic is the step's panic
		run := func(t *testing.T, opts ...ftype.FlowLoopOption) (*executiontype.InMemoryContainer, error) {
			t.Helper()
			h := futura.NewPlainDurableHandle("goroutinePanic", func() *int { v := 0; return &v })
			c := executiontype.NewInMemoryContainer()
			_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewStrict(c)).Execute(t.Context(),
				func(b futura.FlowBuilder, _ struct{}) (string, error) {
					b = h.Provide(b)
					ref := h.Use(b)
					return futura.Source(b, func(ctx context.Context) (string, error) {
						futura.Go(ctx, func(ctx context.Context) {
							*ref = 1
							goroutineThatPanics()
						}).Wait()
						return "ok", nil
					})
				}, struct{}{}, opts...)
			return c, err
		}
		t.Run("nothing is recorded", func(t *testing.T) {
			c, err := run(t)
			assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
			assert.ErrorContains(t, err, "goroutine boom")
			assert.ErrorContains(t, err, "goroutineThatPanics", "the goroutine's own stack is attached")
			assert.Equal(t, 0, c.CallOrderLength())
			_, ok, _ := c.LoadDurable(execution.GenericDurableKey("goroutinePanic"))
			assert.False(t, ok)
		})
		t.Run("a wrapper that recovers cannot turn it into a return", func(t *testing.T) {
			swallowing := fopt.WithStepWrapper(func(ctx context.Context, _ string, _ any, _ []runtime.Frame, call func() (any, error)) error {
				defer func() { recover() }()
				call()
				return nil
			})
			c, err := run(t, swallowing)
			assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
			assert.ErrorContains(t, err, "goroutine boom")
			assert.Equal(t, 0, c.CallOrderLength())
		})
		t.Run("it is not a failure to retry", func(t *testing.T) {
			_, err := run(t, fopt.WithMaxFailures(3))
			assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
			assert.NotErrorIs(t, err, fopt.ErrMaxFailuresReached)
		})
	})
	t.Run("a step blocked on a goroutine's result is released when the goroutine panics", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		c := executiontype.NewInMemoryContainer()
		var cause error
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewStrict(c)).Execute(ctx,
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					res := make(chan string)
					futura.Go(ctx, func(ctx context.Context) { goroutineThatPanics() })
					select {
					case v := <-res:
						return v, nil
					case <-ctx.Done():
						cause = context.Cause(ctx)
						return "", fmt.Errorf("%w", cause)
					}
				})
			}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorContains(t, err, "goroutine boom")
		assert.ErrorContains(t, cause, "goroutine boom", "the step's context is cancelled with the panic")
		assert.Equal(t, 0, c.CallOrderLength(), "the step was recorded")
	})
	t.Run("a goroutine's panic is reported with the step's own exit", func(t *testing.T) {
		for name, exit := range map[string]func() (string, error){
			"error": func() (string, error) { return "", errors.New("step error root cause") },
			"panic": func() (string, error) { panic("step panic root cause") },
		} {
			t.Run(name, func(t *testing.T) {
				_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
					func(b futura.FlowBuilder, _ struct{}) (string, error) {
						return futura.Source(b, func(ctx context.Context) (string, error) {
							futura.Go(ctx, func(ctx context.Context) { panic("goroutine boom") }).Wait()
							return exit()
						})
					}, struct{}{})
				assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
				assert.ErrorContains(t, err, "goroutine boom")
				assert.ErrorContains(t, err, "root cause")
			})
		}
	})
	t.Run("every goroutine's panic is reported, in the order they were started", func(t *testing.T) {
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					var goroutines []futura.Goroutine
					for _, name := range []string{"first boom", "second boom"} {
						goroutines = append(goroutines, futura.Go(ctx, func(ctx context.Context) { panic(name) }))
					}
					for _, g := range goroutines {
						g.Wait()
					}
					return "ok", nil
				})
			}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.Less(t, strings.Index(err.Error(), "first boom"), strings.Index(err.Error(), "second boom"))
	})
	t.Run("a replay terminated while a goroutine runs still restarts", func(t *testing.T) {
		// a Set from the goroutine cancels the replay, and the step observes the cancellation once the
		// goroutine is done: the runtime's own signal, not a panic of the step
		r, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (int, error) {
				s := futura.State(b, 0)
				if s.V() == 0 {
					if err := futura.Action(b, func(ctx context.Context) error {
						futura.Go(ctx, func(ctx context.Context) { s.Set(1) }).Wait()
						return ctx.Err()
					}); err != nil {
						return 0, err
					}
				}
				return s.V(), nil
			}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, 1, r)
	})
	t.Run("a goroutine that panics after restarting the replay still crashes the step", func(t *testing.T) {
		h := futura.NewPlainDurableHandle("panicAfterSet", func() *int { v := 0; return &v })
		c := executiontype.NewInMemoryContainer()
		_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (int, error) {
				b = h.Provide(b)
				ref := h.Use(b)
				s := futura.State(b, 0)
				if s.V() == 0 {
					if err := futura.Action(b, func(ctx context.Context) error {
						futura.Go(ctx, func(ctx context.Context) {
							*ref = 99
							s.Set(1)
							panic("crashed before the second half")
						}).Wait()
						return ctx.Err()
					}); err != nil {
						return 0, err
					}
				}
				return s.V(), nil
			}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorContains(t, err, "crashed before the second half")
		stored, _, _ := c.LoadDurable(execution.GenericDurableKey("panicAfterSet"))
		value, _ := h.Unmarshal(stored)
		assert.Equal(t, 0, *value, "the crashed goroutine's handle write was committed")
	})
	t.Run("a goroutine's panic is reported with a sibling's leak", func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				return futura.Source(b, func(ctx context.Context) (string, error) {
					futura.Go(ctx, func(ctx context.Context) { <-release })
					futura.Go(ctx, func(ctx context.Context) { panic("sibling boom") }).Wait()
					return "ok", nil
				})
			}, struct{}{})
		assert.ErrorIs(t, err, step.ErrGoroutinesNotExited)
		assert.ErrorContains(t, err, "sibling boom")
	})
	t.Run("a goroutine cannot be started from a step that has ended", func(t *testing.T) {
		var stale context.Context
		_, err := futura.NewFlowFromContainer[struct{}, string](containertest.NewInMemory()).Execute(t.Context(),
			func(b futura.FlowBuilder, _ struct{}) (string, error) {
				if _, err := futura.Source(b, func(ctx context.Context) (int, error) { stale = ctx; return 1, nil }); err != nil {
					return "", err
				}
				return futura.Source(b, func(ctx context.Context) (string, error) {
					futura.Go(stale, func(ctx context.Context) {})
					return "ok", nil
				})
			}, struct{}{})
		assert.ErrorIs(t, err, ftrerrors.ErrFlowPanic)
		assert.ErrorIs(t, err, step.ErrStepEnded)
	})
	t.Run("a goroutine that outlives its step and then panics crashes the process", func(t *testing.T) {
		if os.Getenv("FUTURA_TEST_LATE_PANIC") == "1" {
			release := make(chan struct{})
			started := make(chan struct{})
			futura.NewFlowFromContainer[struct{}, int](containertest.NewInMemory()).Execute(context.Background(),
				func(b futura.FlowBuilder, _ struct{}) (int, error) {
					return futura.Source(b, func(ctx context.Context) (int, error) {
						futura.Go(ctx, func(ctx context.Context) { close(started); <-release; panic("late boom") })
						<-started
						return 1, nil
					})
				}, struct{}{})
			close(release)
			time.Sleep(time.Second)
			return
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestGo$/a_goroutine_that_outlives_its_step_and_then_panics_crashes_the_process$")
		cmd.Env = append(os.Environ(), "FUTURA_TEST_LATE_PANIC=1")
		out, err := cmd.CombinedOutput()
		require.Error(t, err, "the late panic was swallowed")
		assert.Contains(t, string(out), "late boom")
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
				started := make(chan struct{})
				_, err := futura.NewFlowFromContainer[struct{}, int](containertest.NewStrict(c)).Execute(t.Context(),
					func(b futura.FlowBuilder, _ struct{}) (int, error) {
						b = h.Provide(b)
						ref := h.Use(b)
						return 0, futura.Action(b, func(ctx context.Context) error {
							futura.Go(ctx, func(ctx context.Context) {
								close(started)
								for {
									select {
									case <-stop:
										return
									default:
										ref.A++
										ref.B++
									}
								}
							})
							<-started
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
}
