package execution

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

	"github.com/futura-platform/futura/flog"
	"github.com/futura-platform/futura/ftype/executiontype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/goroutinebind"
	"github.com/futura-platform/futura/moment"
)

type ctxKey string

const flowExecutionKey ctxKey = "futura_flow"

// FlowExecution is a wrapper around the flow execution state.
// It ensures that whenever the state is accessed/mutated, it is always canonical.
// It allows complex atomic mutations of the state.
// ALL methods for this MUST lock the mutex. It is more than just simply ensuring thread safety. It ensures that the state is always consistent.
type FlowExecution struct {
	mu sync.RWMutex
	c  executiontype.TransactionalContainer
	// running indicates that an execution is currently in flight on this FlowExecution.
	// It gates FromContext: callers cannot reach into the execution before it
	// has started or after it has ended. Protected by mu.
	//
	// NOTE: do not call Running() (which takes mu.RLock) from inside a fn passed
	// to Transact / ReadTransact, as those hold mu.Lock / mu.RLock respectively
	// and sync.RWMutex is not reentrant.
	running bool
}

var ErrTransactionFailed = errors.New("transaction failed")

func (f *FlowExecution) mustTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.c.Transact(ctx, func(ctx context.Context, tx executiontype.Container) error { fn(ctx, tx); return nil })
	if err != nil {
		panic(fmt.Errorf("%w: %w", ErrTransactionFailed, err))
	}
}

func (f *FlowExecution) mustReadTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.ReadOnlyContainer)) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	err := f.c.ReadTransact(ctx, func(ctx context.Context, tx executiontype.ReadOnlyContainer) error { fn(ctx, tx); return nil })
	if err != nil {
		panic(fmt.Errorf("%w: %w", ErrTransactionFailed, err))
	}
}

func NewFlowExecution() *FlowExecution {
	return &FlowExecution{
		c: executiontype.NewInMemoryContainer(),
	}
}

func NewFlowExecutionWithContainer(c executiontype.TransactionalContainer) *FlowExecution {
	return &FlowExecution{
		c: c,
	}
}

func WithFlow(ctx context.Context, exec *FlowExecution) context.Context {
	if exec == nil {
		panic("flow execution cannot be nil")
	}
	return context.WithValue(goroutinebind.BindGoroutine(ctx), flowExecutionKey, exec)
}

var (
	ErrNoCurrentReplay      = errors.New("no current replay")
	ErrNilCancellationCause = errors.New("current replay cancellation cause cannot be nil")

	ErrRestartReplay = errors.New("restarting replay")
)

// restartCurrentReplay cancels the current replay, which will always start a new one.
// (Regardless of whether or not the last replay was successful).
// This will also skip the default end of replay behavior, including rewinding the sequence index and resetting the replay flags.
func (f *FlowExecution) restartCurrentReplay(ctx context.Context, cause error) {
	if !replay.Has(ctx) {
		panic(ftrerrors.InconsistentStateError(ErrNoCurrentReplay))
	} else if cause == nil {
		panic(ftrerrors.InconsistentStateError(ErrNilCancellationCause))
	}
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "restarting replay",
		slog.String("cause", cause.Error()),
	)
	replay.Cancel(ctx, fmt.Errorf("%w: %w", ErrRestartReplay, cause))
}

var DefaultReplayFlags = replay.Flags{
	PanicOnMomentOrderChange: true,
}

var (
	GenericDurableKey                  = namespacedDurableKeyConstructor("generic")
	sequenceEpochDurableKeyConstructor = namespacedDurableKeyConstructor("sequence_epoch")
	dirtyEpochKey                      = sequenceEpochDurableKeyConstructor("dirty")
	evaluatedEpochKey                  = sequenceEpochDurableKeyConstructor("evaluated")
)

func (f *FlowExecution) getEpoch(tx executiontype.Container, key string) uint64 {
	encoded, ok, err := tx.LoadDurable(key)
	if err != nil {
		panic(err)
	}
	var epoch uint64
	if ok {
		if _, err := binary.Decode(encoded, binary.LittleEndian, &epoch); err != nil {
			panic(err)
		}
	}

	return epoch
}
func (f *FlowExecution) setEpoch(tx executiontype.Container, key string, epoch uint64) {
	encoded, err := binary.Append(nil, binary.LittleEndian, epoch)
	if err != nil {
		panic(err)
	}
	err = tx.StoreDurable(key, encoded)
	if err != nil {
		panic(err)
	}
}

func (f *FlowExecution) StartNewReplay(ctx context.Context) (context.Context, uint64) {
	flags := DefaultReplayFlags
	var dirtyEpoch uint64
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		dirtyEpoch = f.getEpoch(tx, dirtyEpochKey)
		epoch := f.getEpoch(tx, evaluatedEpochKey)
		if dirtyEpoch > epoch {
			// we are in un charted territory, so allow moment order to change
			flags.PanicOnMomentOrderChange = false
		}
	})

	return sequence.With(replay.With(ctx), flags), dirtyEpoch
}

func (f *FlowExecution) SettleSequence(ctx context.Context, dirtyEpoch uint64) {
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		f.setEpoch(tx, evaluatedEpochKey, dirtyEpoch)
		tx.TruncateCallOrderAt(sequence.GetIndex(ctx))
	})
}

func namespacedDurableKeyConstructor(namespace string) func(key string) string {
	return func(key string) string {
		return namespace + ":" + key
	}
}

func (f *FlowExecution) InvalidateSequence(ctx context.Context, cause error) {
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		epoch := f.getEpoch(tx, dirtyEpochKey)
		f.setEpoch(tx, dirtyEpochKey, epoch+1)
	})
	f.restartCurrentReplay(ctx, cause)
}

func (f *FlowExecution) ExpectedIdentity(ctx context.Context) (identity moment.Identity, ok bool) {
	i := sequence.GetIndex(ctx)
	f.mustReadTransact(ctx, func(ctx context.Context, tx executiontype.ReadOnlyContainer) {
		size := tx.CallOrderLength()
		if i > size {
			panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
				sequenceIndex:  i,
				sequenceLength: size,
			}))
		} else if i == size {
			return
		}

		identity = tx.CallOrderAt(i)
		ok = true
	})
	return identity, ok
}

func (f *FlowExecution) GetMoment(ctx context.Context, identity moment.Identity) (moment.Moment, bool) {
	var moment moment.Moment
	var ok bool
	f.mustReadTransact(ctx, func(ctx context.Context, tx executiontype.ReadOnlyContainer) {
		moment, ok = tx.GetMoment(identity)
	})
	return moment, ok
}

// RecordCurrentMoment stores the current identity+moment (growing the sequence slice if necessary)
// it also marks the identity as seen.
func (f *FlowExecution) RecordCurrentMoment(ctx context.Context, identity moment.Identity, currentMoment moment.Moment) {
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		if sequence.IsSeen(ctx, identity) {
			// if we see an identity twice in the same replay, the consumer is doing something wrong
			panic(ftrerrors.InconsistentStateError(UnexpectedDuplicateMomentError{
				identity: identity,
			}))
		}

		i := sequence.GetIndex(ctx)
		size := tx.CallOrderLength()
		if i > size {
			panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
				sequenceIndex:  i,
				sequenceLength: size,
			}))
		} else if i == size {
			tx.AppendCallOrder(identity)
		} else {
			tx.SetCallOrderAt(i, identity)
		}
		tx.SetMoment(identity, currentMoment)
		sequence.MarkSeen(ctx, identity)
	})
}

func (f *FlowExecution) LoadDurable(ctx context.Context, durableKey string) ([]byte, bool) {
	var state []byte
	var ok bool
	var err error
	f.mustReadTransact(ctx, func(ctx context.Context, tx executiontype.ReadOnlyContainer) {
		state, ok, err = tx.LoadDurable(GenericDurableKey(durableKey))
	})
	if err != nil {
		panic(err)
	}
	return state, ok
}

func (f *FlowExecution) StoreDurable(ctx context.Context, durableKey string, state []byte) {
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		err := tx.StoreDurable(GenericDurableKey(durableKey), state)
		if err != nil {
			panic(err)
		}
	})
}

// Running reports whether an execution is currently active on this FlowExecution.
// It is set to true by TryStartRun and back to false by the stop func it returns.
func (f *FlowExecution) Running() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.running
}

// TryStartRun marks this FlowExecution as running. It returns (stop, true)
// on success; stop must be called (typically deferred) to mark the run as ended.
// If a run is already in progress, it returns (nil, false).
func (f *FlowExecution) TryStartRun() (stop func(), ok bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running {
		return nil, false
	}
	f.running = true
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.running = false
	}, true
}

// ErrFlowExecutionNotRunning is reported when code attempts to access a
// FlowExecution via the context outside of an active run.
// Most commonly this means a closure captured during a flow (e.g. a durable
// handle's persist func) was invoked after the flow returned.
var ErrFlowExecutionNotRunning = errors.New("flow execution is not running")

// UnsafeFromContext returns the FlowExecution stored on ctx without performing
// the goroutine-binding or running-state assertions that FromContext does.
//
// This exists exclusively for test helpers that drive a FlowExecution outside
// of the production entry point and therefore need to reach the exec before
// TryStartRun has been called. It panics outside of a test binary so it
// cannot be reached for in production code by accident.
func UnsafeFromContext(ctx context.Context) *FlowExecution {
	if !testing.Testing() {
		panic("execution.UnsafeFromContext is test-only; use FromContext instead")
	}
	v, _ := ctx.Value(flowExecutionKey).(*FlowExecution)
	return v
}

// FromContext retrieves the flow context from the context.
// It will panic if:
//   - the context is being used in a goroutine other than the one that created it.
//   - the FlowExecution is not currently running (i.e. before TryStartRun or
//     after the matching stop has fired).
func FromContext(ctx context.Context) (*FlowExecution, bool) {
	v, ok := ctx.Value(flowExecutionKey).(*FlowExecution)
	if ok {
		if err := goroutinebind.AssertBoundGoroutine(ctx); err != nil {
			panic(ftrerrors.InconsistentStateError(err))
		}
		if !v.Running() {
			panic(ftrerrors.InconsistentStateError(ErrFlowExecutionNotRunning))
		}
	}
	return v, ok
}

var ErrFlowContextNotFound = errors.New("flowContext not found in context")

func MustFromContext(ctx context.Context) *FlowExecution {
	f, ok := FromContext(ctx)
	if !ok {
		panic(ErrFlowContextNotFound)
	}
	return f
}

type SequenceIndexOutOfBoundsError struct {
	sequenceIndex  int
	sequenceLength int
}

func (e SequenceIndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("sequenceIndex is greater than the length of the memoized moment sequence: %d > %d", e.sequenceIndex, e.sequenceLength)
}

type UnexpectedDuplicateMomentError struct {
	identity moment.Identity
}

func (e UnexpectedDuplicateMomentError) Error() string {
	return fmt.Sprintf("identity '%s' was seen twice in the same replay", e.identity.String())
}

// FlowContextUsedInWrongGoroutineError is an error used to enforce a flow executing all within the same goroutine.
type FlowContextUsedInWrongGoroutineError struct {
	createdInGoroutineID int64
	usedInGoroutineID    int64
}

func (e FlowContextUsedInWrongGoroutineError) Error() string {
	return fmt.Sprintf("flowContext created in goroutine %d was used in goroutine %d", e.createdInGoroutineID, e.usedInGoroutineID)
}
