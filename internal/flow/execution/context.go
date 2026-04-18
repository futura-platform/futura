package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

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
	mu        sync.RWMutex
	nextFlags replay.Flags
	c         executiontype.TransactionalContainer
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
		c:         executiontype.NewInMemoryContainer(),
		nextFlags: DefaultReplayFlags,
	}
}

func NewFlowExecutionWithContainer(c executiontype.TransactionalContainer) *FlowExecution {
	return &FlowExecution{
		c:         c,
		nextFlags: DefaultReplayFlags,
	}
}

func WithFlow(ctx context.Context, exec *FlowExecution) context.Context {
	if exec == nil {
		panic("flow execution cannot be nil")
	}
	return context.WithValue(goroutinebind.BindGoroutine(ctx), flowExecutionKey, exec)
}

func (f *FlowExecution) EvictUnseenCachedMoments(ctx context.Context) {
	l := flog.FromContext(ctx)
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		for identity := range tx.KnownMoments() {
			if !sequence.IsSeen(ctx, identity) {
				tx.DeleteMoment(identity)
				l.LogAttrs(ctx, slog.LevelDebug, "evicted unseen state",
					slog.String("identity", identity.String()),
				)
			}
		}
	})
}

var (
	ErrNoCurrentReplay      = errors.New("no current replay")
	ErrNilCancellationCause = errors.New("current replay cancellation cause cannot be nil")

	ErrRestartReplay = errors.New("restarting replay")
)

// RestartCurrentReplay cancels the current replay, which will always start a new one.
// (Regardless of whether or not the last replay was successful).
// This will also skip the default end of replay behavior, including rewinding the sequence index and resetting the replay flags.
func (f *FlowExecution) RestartCurrentReplay(ctx context.Context, cause error) {
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

func (f *FlowExecution) StartNewReplay(ctx context.Context) context.Context {
	f.mu.Lock()
	defer f.mu.Unlock()

	defer func() {
		// reset to default flags
		f.nextFlags = DefaultReplayFlags
	}()
	return sequence.With(replay.With(ctx), f.nextFlags)
}

func (f *FlowExecution) SetNextFlags(fn func(flags *replay.Flags)) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fn(&f.nextFlags)
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
		i := sequence.GetIndex(ctx)
		size := tx.CallOrderLength()
		if i > size {
			panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
				sequenceIndex:  i,
				sequenceLength: size,
			}))
		} else if i == size {
			ok := tx.HasMoment(identity)
			if ok {
				panic(ftrerrors.InconsistentStateError(UnexpectedCachedStateError{
					identity: identity,
				}))
			}
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
		state, ok, err = tx.LoadDurable(durableKey)
	})
	if err != nil {
		panic(err)
	}
	return state, ok
}

func (f *FlowExecution) StoreDurable(ctx context.Context, durableKey string, state []byte) {
	f.mustTransact(ctx, func(ctx context.Context, tx executiontype.Container) {
		err := tx.StoreDurable(durableKey, state)
		if err != nil {
			panic(err)
		}
	})
}

// FromContext retrieves the flow context from the context.
// It will panic if this is being used in a goroutine other than the one that created the context.
func FromContext(ctx context.Context) (*FlowExecution, bool) {
	v, ok := ctx.Value(flowExecutionKey).(*FlowExecution)
	if ok {
		if err := goroutinebind.AssertBoundGoroutine(ctx); err != nil {
			panic(ftrerrors.InconsistentStateError(err))
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

type UnexpectedCachedStateError struct {
	identity moment.Identity
}

func (e UnexpectedCachedStateError) Error() string {
	return fmt.Sprintf("identity '%s' exists in the state cache, but was not expected to be present", e.identity.String())
}

// FlowContextUsedInWrongGoroutineError is an error used to enforce a flow executing all within the same goroutine.
type FlowContextUsedInWrongGoroutineError struct {
	createdInGoroutineID int64
	usedInGoroutineID    int64
}

func (e FlowContextUsedInWrongGoroutineError) Error() string {
	return fmt.Sprintf("flowContext created in goroutine %d was used in goroutine %d", e.createdInGoroutineID, e.usedInGoroutineID)
}
