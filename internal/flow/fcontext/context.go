package fcontext

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/flog"
	"github.com/futura-platform/futura/ftype"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/petermattis/goid"
)

type ctxKey string

const flowContextKey ctxKey = "futura_flow"

type ReplayFlags struct {
	PanicOnMomentOrderChange bool
}
type flowContext struct {
	options            []ftype.FlowLoopOption
	replayFlags        ReplayFlags
	creatorGoroutineID int64

	cancelCurrentReplay context.CancelCauseFunc

	// the current state of the flow.
	stateCache map[moment.Identity]*moment.Moment
	// this includes the callpaths that have not been seen yet in the current replay.
	unseenCachedCallpaths mapset.Set[moment.Identity]

	// given a certain state, this sequence should be deterministic.
	callOrder     []moment.Identity
	sequenceIndex int
}

func WithFlow(ctx context.Context, options []ftype.FlowLoopOption) context.Context {
	return context.WithValue(ctx, flowContextKey, &flowContext{
		options:            options,
		creatorGoroutineID: goid.Get(),
		stateCache:         make(map[moment.Identity]*moment.Moment),
	})
}

func (f *flowContext) Options() []ftype.FlowLoopOption {
	return f.options
}

func (f *flowContext) Rewind() {
	f.sequenceIndex = 0
}

func (f *flowContext) SequenceIndex() int {
	return f.sequenceIndex
}

func (f *flowContext) EvictUnseenCachedStates(ctx context.Context) {
	l := flog.FromContext(ctx)
	for identity := range mapset.Elements(f.unseenCachedCallpaths) {
		delete(f.stateCache, identity)
		l.LogAttrs(ctx, slog.LevelDebug, "evicted cached state",
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
func (f *flowContext) RestartCurrentReplay(ctx context.Context, cause error) {
	if f.cancelCurrentReplay == nil {
		panic(ftrerrors.InconsistentStateError(ErrNoCurrentReplay))
	} else if cause == nil {
		panic(ftrerrors.InconsistentStateError(ErrNilCancellationCause))
	}
	l := flog.FromContext(ctx)
	l.LogAttrs(ctx, slog.LevelDebug, "restarting replay",
		slog.String("cause", cause.Error()),
	)
	f.cancelCurrentReplay(fmt.Errorf("%w: %w", ErrRestartReplay, cause))
}

func (f *flowContext) StartNewReplay(ctx context.Context) (context.Context, context.CancelCauseFunc) {
	f.resetUnseenCachedCallpaths()
	replayCtx, cancel := context.WithCancelCause(ctx)
	f.cancelCurrentReplay = cancel
	return replayCtx, cancel
}

func (f *flowContext) resetUnseenCachedCallpaths() {
	f.unseenCachedCallpaths = mapset.NewSetWithSize[moment.Identity](len(f.stateCache))
	for callpath := range f.stateCache {
		f.unseenCachedCallpaths.Add(callpath)
	}
}

func (f *flowContext) ReplayFlags() ReplayFlags {
	return f.replayFlags
}

func (f *flowContext) SetReplayFlags(flags func(flags *ReplayFlags)) {
	flags(&f.replayFlags)
}

func (f *flowContext) ExpectedIdentity() (moment.Identity, bool) {
	if f.sequenceIndex > len(f.callOrder) {
		panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  f.sequenceIndex,
			sequenceLength: len(f.callOrder),
		}))
	} else if f.sequenceIndex == len(f.callOrder) {
		return moment.Identity{}, false
	}

	identity := f.callOrder[f.sequenceIndex]
	return identity, true
}

func (f *flowContext) GetMoment(identity moment.Identity) (*moment.Moment, bool) {
	moment, ok := f.stateCache[identity]
	return moment, ok
}

func (f *flowContext) InvalidateMoment(identity moment.Identity) {
	delete(f.stateCache, identity)
}

// RecordCurrentMoment stores the current identity+moment (growing the sequence slice if necessary)
// it also marks the identity as seen.
func (f *flowContext) RecordCurrentMoment(identity moment.Identity, currentMoment *moment.Moment) {
	if f.sequenceIndex > len(f.callOrder) {
		panic(ftrerrors.InconsistentStateError(SequenceIndexOutOfBoundsError{
			sequenceIndex:  f.sequenceIndex,
			sequenceLength: len(f.callOrder),
		}))
	} else if f.sequenceIndex == len(f.callOrder) {
		_, ok := f.stateCache[identity]
		if ok {
			panic(ftrerrors.InconsistentStateError(UnexpectedCachedStateError{
				identity: identity,
			}))
		}
		f.callOrder = append(f.callOrder, identity)
	} else {
		f.callOrder[f.sequenceIndex] = identity
	}
	f.unseenCachedCallpaths.Remove(identity)
	f.stateCache[identity] = currentMoment
}

// Advance advances the sequence index by 1.
func (f *flowContext) Advance() {
	f.sequenceIndex++
}

// FromContext retrieves the flow context from the context.
// It will panic if this is being used in a goroutine other than the one that created the context.
func FromContext(ctx context.Context) (*flowContext, bool) {
	v, ok := ctx.Value(flowContextKey).(*flowContext)
	if ok && v.creatorGoroutineID != goid.Get() {
		panic(ftrerrors.InconsistentStateError(FlowContextUsedInWrongGoroutineError{
			createdInGoroutineID: v.creatorGoroutineID,
			usedInGoroutineID:    goid.Get(),
		}))
	}
	return v, ok
}

var ErrFlowContextNotFound = errors.New("flowContext not found in context")

func MustFromContext(ctx context.Context) *flowContext {
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

type FlowContextUsedInWrongGoroutineError struct {
	createdInGoroutineID int64
	usedInGoroutineID    int64
}

func (e FlowContextUsedInWrongGoroutineError) Error() string {
	return fmt.Sprintf("flowContext created in goroutine %d was used in goroutine %d", e.createdInGoroutineID, e.usedInGoroutineID)
}
