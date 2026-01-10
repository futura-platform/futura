package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/futura-platform/futura/flog"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/goroutinebind"
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
	s         FlowExecutionState
}

// State returns the flow execution state.
// Values returned by this method are gauranteed to be canonical.
func (f *FlowExecution) State() FlowExecutionState {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.s
}

// FlowExecutionState is the state of the flow execution.
// All values here should be usable between program instances (i.e. no unsafe pointers, functions, goroutine ids, etc.).
// This type is designed to be serialized and deserialized to facilitate distributed execution.
type FlowExecutionState struct {
	// a map of the step moment identifiers to their memoized moment.
	stateCache map[moment.Identity]*moment.Moment

	// given a certain state, this sequence should be deterministic.
	callOrder []moment.Identity
}

func NewFlowExecution() *FlowExecution {
	return &FlowExecution{
		s: FlowExecutionState{
			stateCache: make(map[moment.Identity]*moment.Moment),
		},
		nextFlags: DefaultReplayFlags,
	}
}

func NewFlowExecutionFromState(s FlowExecutionState) *FlowExecution {
	return &FlowExecution{
		s:         s,
		nextFlags: DefaultReplayFlags,
	}
}

func WithFlow(ctx context.Context, exec *FlowExecution) context.Context {
	if exec == nil {
		panic("flow execution cannot be nil")
	}
	return context.WithValue(goroutinebind.BindGoroutine(ctx), flowExecutionKey, exec)
}

func (f *FlowExecution) EvictUnseenCachedStates(replayCtx context.Context) {
	l := flog.FromContext(replayCtx)
	for identity := range f.s.stateCache {
		if sequence.IsSeen(replayCtx, identity) {
			continue
		}
		delete(f.s.stateCache, identity)
		l.LogAttrs(replayCtx, slog.LevelDebug, "evicted cached state",
			slog.String("identity", identity.String()),
		)
	}
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

func (f *FlowExecution) ExpectedIdentity(ctx context.Context) (moment.Identity, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	i := sequence.GetIndex(ctx)
	if i > len(f.s.callOrder) {
		panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  i,
			sequenceLength: len(f.s.callOrder),
		}))
	} else if i == len(f.s.callOrder) {
		return moment.Identity{}, false
	}

	identity := f.s.callOrder[i]
	return identity, true
}

func (f *FlowExecution) GetMoment(identity moment.Identity) (*moment.Moment, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	moment, ok := f.s.stateCache[identity]
	return moment, ok
}

func (f *FlowExecution) InvalidateMoment(identity moment.Identity) {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.s.stateCache, identity)
}

// RecordCurrentMoment stores the current identity+moment (growing the sequence slice if necessary)
// it also marks the identity as seen.
func (f *FlowExecution) RecordCurrentMoment(ctx context.Context, identity moment.Identity, currentMoment *moment.Moment) {
	f.mu.Lock()
	defer f.mu.Unlock()

	i := sequence.GetIndex(ctx)
	if i > len(f.s.callOrder) {
		panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  i,
			sequenceLength: len(f.s.callOrder),
		}))
	} else if i == len(f.s.callOrder) {
		_, ok := f.s.stateCache[identity]
		if ok {
			panic(ftrerrors.InconsistentStateError(UnexpectedCachedStateError{
				identity: identity,
			}))
		}
		f.s.callOrder = append(f.s.callOrder, identity)
	} else {
		f.s.callOrder[i] = identity
	}
	sequence.MarkSeen(ctx, identity)
	f.s.stateCache[identity] = currentMoment
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
