package step

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"

	"github.com/futura-platform/futura/flog"
	ftrerrors "github.com/futura-platform/futura/internal/errors"
	"github.com/futura-platform/futura/internal/flow/execution"
	"github.com/futura-platform/futura/internal/flow/replay"
	"github.com/futura-platform/futura/internal/flow/replay/sequence"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
	"k8s.io/utils/diff"
)

var (
	ErrEvaledOutsideOfAFlowFunction = errors.New("steps cannot be evaluated outside of a replay function")
	ErrNestedStep                   = errors.New("steps cannot be evaluated from inside another step")
	ErrStepAfterFailure             = errors.New("steps cannot be evaluated after another step failed in the same replay")
	ErrUnexpectedBranchTaken        = errors.New("unexpected branch taken, new branches should only be triggered by a futura state change")
)

func init() {
	// Evaluate is the single point at which user callstacks are captured for moment identities.
	replay.SetCaptureFunction(Evaluate[struct{}, struct{}])
}

func Evaluate[A comparable, R any](
	ctx context.Context,
	fn moment.Fn[A, R],
	args A,
) (output R, err error) {
	if err = testutil.InjectedError(ctx, testutil.InjectedErrorLevelEvaluate); err != nil {
		return
	}

	callstack, ok := replay.GetClosestReplayUserCallstack()
	if !ok {
		panic(ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction))
	}

	return evaluateWithCallstack(
		ctx,
		fn,
		args,
		callstack,
	)
}

var ErrEvalFailed = errors.New("eval failed")

func evaluateWithCallstack[A comparable, R any](
	ctx context.Context,
	fn moment.Fn[A, R],
	args A,
	callstack []runtime.Frame,
) (output R, err error) {
	if !replay.Has(ctx) {
		panic(ftrerrors.InconsistentStateError(ErrEvaledOutsideOfAFlowFunction))
	}
	// if the replay is cancelled, terminate it immediately
	terminateIfReplayCancelled(ctx)

	if sequence.IsEvaluating(ctx) {
		panic(ftrerrors.InconsistentStateError(ErrNestedStep))
	}
	if sequence.HasFailed(ctx) {
		panic(ftrerrors.InconsistentStateError(ErrStepAfterFailure))
	}
	identity := moment.NewIdentity(
		ctx,
		replay.CallstackToCallpath(callstack),
		replay.FuncToCallsite(fn.RuntimeFunc()),
	)

	f := execution.MustFromContext(ctx)
	// an identity reached twice in one replay can never be memoized, so it is rejected before its fn runs
	if sequence.IsSeen(ctx, identity) {
		panic(ftrerrors.InconsistentStateError(execution.UnexpectedDuplicateMomentError{Identity: identity}))
	}
	l := flog.FromContext(ctx)

	thisSequenceIndex := sequence.GetIndex(ctx)
	var cacheStatus string = "MISS"
	defer func() {
		r := recover()
		l.LogAttrs(ctx, slog.LevelDebug, "evaluated step",
			slog.String("label", fn.Label()),
			slog.Int("index", thisSequenceIndex),
			slog.String("cache_status", cacheStatus),
			slog.String("error", fmt.Sprint(err)),
		)
		// any exit but a returned output leaves the replay unable to continue past this step
		if r != nil || err != nil {
			sequence.MarkFailed(ctx)
		}
		if r != nil {
			// a termination is the runtime's own signal should pass through untouched
			if _, terminated := AsReplayTerminated(r); terminated {
				panic(r)
			}
			panic(fmt.Errorf("%s: %w", fn.Label(), ftrerrors.PanicError(r)))
		} else if err != nil {
			err = fmt.Errorf("%s: %w: %w", fn.Label(), ErrEvalFailed, err)
		}
	}()

	// first check if the expected identity is the same as the current identity,
	// if nothing is expected (meaning if ok is false), we can continue
	expectedIdentity, ok := f.ExpectedIdentity(ctx)
	if ok && expectedIdentity != identity && sequence.GetFlags(ctx).PanicOnMomentOrderChange {
		panic(ftrerrors.InconsistentStateError(fmt.Errorf(
			"%w:\n%s",
			ErrUnexpectedBranchTaken,
			diff.ObjectGoPrintSideBySide(expectedIdentity.String(), identity.String()),
		)))
	}

	// register input and output types so moments can be properly serialized/deserialized
	// (ignore for interfaces, we expect them to be registered by the caller)
	inputRt := reflect.TypeFor[A]()
	if inputRt.Kind() != reflect.Interface {
		privateencoding.RegisterType(inputRt)
	}
	outputRt := reflect.TypeFor[R]()
	if outputRt.Kind() != reflect.Interface {
		privateencoding.RegisterType(outputRt)
	}

	// then get the moment from the cache
	currentMomentValue, ok := f.GetMoment(ctx, identity)
	currentMoment := &currentMomentValue

	// validate BEFORE deferring the output handler, so that in the event of a panic, nothing is recorded.
	needsExecution := !ok || !currentMoment.Validate(args)
	if needsExecution {
		// the recorded moment (if any) is for a stale input, so record the execution against a fresh one.
		currentMoment = moment.NewMoment(args)
	}
	defer func() {
		// a step that may still be running could still have writers running,
		// so we should check here before recording to make sure we don't commit inconsistent state
		r := recover()
		if rerr, ok := r.(error); ok && errors.Is(rerr, ErrStillRunning) {
			panic(r)
		}
		if err != nil {
			currentMoment.Invalidate()
		}
		// handle the result of the step, if it was successful
		if recordErr := ftrerrors.Recovering(func() error {
			f.RecordCurrentMoment(ctx, identity, *currentMoment)
			return nil
		}); recordErr != nil {
			// do a best effort to join the record error with the step's panic, if any
			if _, terminated := AsReplayTerminated(r); r == nil || terminated {
				panic(recordErr)
			}
			panic(fmt.Errorf("%w: %w", ftrerrors.PanicError(r), recordErr))
		}
		if r != nil {
			panic(r)
		}
		if err != nil {
			return
		}
		sequence.Advance(ctx)
	}()
	// validate it. If it no longer valid, re execute it and update the cache
	if needsExecution {
		done := sequence.MarkEvaluating(ctx)
		defer done()
		output, err = call(ctx, fn, identity, args, callstack)
		if err != nil {
			return
		}
		currentMoment.SetValidOutput(output)
	} else {
		cacheStatus = "HIT"
	}

	// return the memoized result
	anyOutput, ok := currentMoment.Output().Get()
	if !ok {
		panic(ftrerrors.InconsistentStateError(fmt.Errorf("expected moment to have a valid output, but it was not")))
	}
	if anyOutput == nil {
		return output, nil
	}
	return anyOutput.(R), nil
}
