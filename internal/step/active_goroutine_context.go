package step

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	mapset "github.com/deckarep/golang-set/v2"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
)

type contextKey string

const (
	activeGoroutinesContextKey contextKey = "active_goroutines"
)

var ErrStepEnded = errors.New("the step has ended")

// ActiveGoroutines is the register of a step's goroutines: which are still running, and what any of
// them panicked with. The step may not end until it is empty, and a goroutine's panic is the step's.
type ActiveGoroutines struct {
	// mu makes a goroutine's exit and the step's end one decision: exactly one of them reports the panic.
	mu         sync.Mutex
	next       int64
	ended      bool
	running    mapset.Set[int64]
	panics     map[int64]error
	cancelStep context.CancelCauseFunc
}

// Start registers a goroutine before it runs. done unregisters it with what it panicked with, if
// anything, and closes exited. It panics if the step has ended: nothing would report the goroutine.
func (g *ActiveGoroutines) Start() (done func(panicked error), exited <-chan struct{}) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.ended {
		panic(ftrerrors.InconsistentStateError(ErrStepEnded))
	}
	g.next++
	id := g.next
	g.running.Add(id)
	ch := make(chan struct{})
	return func(panicked error) {
		g.mu.Lock()
		g.running.Remove(id)
		if panicked != nil {
			g.panics[id] = panicked
			// release the step from being blocked by this goroutine's panic
			// (since it should be waiting for this goroutine to exit)
			g.cancelStep(panicked)
		}
		ended := g.ended
		g.mu.Unlock()
		close(ch)
		if ended && panicked != nil {
			// the step ended without waiting: nothing reports the panic any more, so it is not swallowed
			panic(panicked)
		}
	}, ch
}

// End closes the register: the step is over. It returns what the goroutines raised, or nil: a crash
// (their panics joined, in the order they were started) beats a leak, which beats a termination,
// which is returned as it is.
func (g *ActiveGoroutines) End() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ended = true
	var crashed error
	var terminated error
	for _, id := range slices.Sorted(maps.Keys(g.panics)) {
		panicked := g.panics[id]
		if _, isTermination := AsReplayTerminated(panicked); isTermination {
			terminated = panicked
			continue
		}
		crashed = errors.Join(crashed, panicked)
	}
	switch {
	case g.running.Cardinality() != 0:
		return errors.Join(fmt.Errorf("%w: %d still running", ErrGoroutinesNotExited, g.running.Cardinality()), crashed)
	case crashed != nil:
		return crashed
	default:
		return terminated
	}
}

// withActiveGoroutines derives the step's context, which its register cancels when a goroutine panics.
func withActiveGoroutines(ctx context.Context) (*ActiveGoroutines, context.Context) {
	ctx, cancel := context.WithCancelCause(ctx)
	g := &ActiveGoroutines{running: mapset.NewThreadUnsafeSet[int64](), panics: map[int64]error{}, cancelStep: cancel}
	return g, context.WithValue(ctx, activeGoroutinesContextKey, g)
}

var ErrGoOutsideOfAStep = errors.New("goroutines can only be started from inside a step")

// ActiveGoroutinesFrom returns the step's register.
func ActiveGoroutinesFrom(ctx context.Context) *ActiveGoroutines {
	g, ok := ctx.Value(activeGoroutinesContextKey).(*ActiveGoroutines)
	if !ok {
		panic(ftrerrors.InconsistentStateError(ErrGoOutsideOfAStep))
	}
	return g
}
