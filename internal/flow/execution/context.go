package execution

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"

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
	// pendingInvalidation holds the mutations that invalidate the sequence, in a durable form.
	pendingInvalidation []func(tx executiontype.Container)
	// cancelCurrentReplay cancels the most recently started replay. Protected by mu.
	cancelCurrentReplay context.CancelCauseFunc
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
	ErrNilCancellationCause = errors.New("current replay cancellation cause cannot be nil")

	ErrRestartReplay = errors.New("restarting replay")
)

// RestartCurrentReplay cancels the current replay, which will always start a new one.
// (Regardless of whether or not the last replay was successful).
// This will also skip the default end of replay behavior, including rewinding the sequence index and resetting the replay flags.
func (f *FlowExecution) RestartCurrentReplay(cause error) {
	if cause == nil {
		panic(ftrerrors.InconsistentStateError(ErrNilCancellationCause))
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cancelCurrentReplay == nil {
		return
	}

	slog.Debug("restarting replay", slog.String("cause", cause.Error()))
	f.cancelCurrentReplay(fmt.Errorf("%w: %w", ErrRestartReplay, cause))
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

// StartNewReplay begins a replay. Any pending invalidation is committed first
func (f *FlowExecution) StartNewReplay(ctx context.Context) (context.Context, uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	replayCtx, cancel := replay.With(ctx)
	f.cancelCurrentReplay = cancel

	// The transaction may be retried by the container, so it only reads and writes durable state.
	// In-memory state (the pending queue) is consumed after it commits.
	var flags replay.Flags
	var dirtyEpoch uint64
	err := f.c.Transact(ctx, func(ctx context.Context, tx executiontype.Container) error {
		dirtyEpoch = f.getEpoch(tx, dirtyEpochKey)
		if len(f.pendingInvalidation) > 0 {
			dirtyEpoch++
			f.setEpoch(tx, dirtyEpochKey, dirtyEpoch)
			for _, write := range f.pendingInvalidation {
				write(tx)
			}
		}
		flags = replay.Flags{
			// an unevaluated invalidation is uncharted territory, so allow the moment order to change
			PanicOnMomentOrderChange: dirtyEpoch <= f.getEpoch(tx, evaluatedEpochKey),
		}
		return nil
	})
	if err != nil {
		panic(fmt.Errorf("%w: %w", ErrTransactionFailed, err))
	}
	f.pendingInvalidation = nil

	return sequence.With(replayCtx, flags), dirtyEpoch
}

var (
	ErrEpochRegression         = errors.New("the evaluated epoch can never move backwards")
	ErrSettledSequenceMismatch = errors.New("a settled sequence must end exactly where the replay stopped")
)

func (f *FlowExecution) SettleSequence(ctx context.Context, atIndex int, toEpoch uint64) {
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		stored := f.getEpoch(tx, evaluatedEpochKey)
		if stored > toEpoch {
			panic(ftrerrors.InconsistentStateError(fmt.Errorf("%w: stored %d, settling to %d", ErrEpochRegression, stored, toEpoch)))
		}
		tx.TruncateCallOrderAt(atIndex)
		if length := tx.CallOrderLength(); length != atIndex+1 {
			panic(ftrerrors.InconsistentStateError(fmt.Errorf("%w: the call order has %d entries, but the replay recorded %d", ErrSettledSequenceMismatch, length, atIndex+1)))
		}
		if stored != toEpoch {
			f.setEpoch(tx, evaluatedEpochKey, toEpoch)
		}
	})
}

func namespacedDurableKeyConstructor(namespace string) func(key string) string {
	return func(key string) string {
		return namespace + ":" + key
	}
}

// InvalidateSequence records a pending invalidation of the sequence, through the apply func, behind the execution's lock.
// write is called with the transaction that commits it, so that the invalidation is never made durable without its mutation.
// Nothing is written until the next StartNewReplay call.
func (f *FlowExecution) InvalidateSequence(apply func(), write func(tx executiontype.Container)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	apply()
	f.pendingInvalidation = append(f.pendingInvalidation, write)
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
