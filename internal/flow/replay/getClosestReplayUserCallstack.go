package replay

import (
	"errors"
	"runtime"
	"slices"

	"github.com/futura-platform/futura/internal/flow/replay/calliter"
)

var ErrNoCaptureFrame = errors.New("the callstack was captured outside of the capture function")

// captureFunction is the fully qualified runtime name of the single function that is
// allowed to capture a user callstack. Frames at or below it are excluded from the callstack,
// so that identities are only ever derived from the frames above it.
var captureFunction string

// SetCaptureFunction pins the function whose frame marks the start of the user callstack.
// It must be called exactly once, before any callstack is captured.
func SetCaptureFunction(fn any) {
	if captureFunction != "" {
		panic("the capture function has already been set")
	}
	captureFunction = runtimeFunctionName(fn)
}

// GetClosestReplayUserCallstack returns the user frames between the capture function and
// the closest replay execution, in call order.
//
// It panics if the capture function is not on the stack, as that means an identity is being
// derived from somewhere other than the pinned capture point.
func GetClosestReplayUserCallstack() ([]runtime.Frame, bool) {
	if captureFunction == "" {
		panic("the capture function has not been set")
	}

	var accumulatedCallstack []runtime.Frame
	capturing := false
	for frame := range calliter.NewFrameIter(8, 0) {
		if !capturing {
			// everything at or below the capture function is framework plumbing
			capturing = frame.Function == captureFunction
			continue
		}
		if isExecuteReplayCallFlowFrame(frame) {
			// the callpath is built in reverse order, so we need to reverse it before returning
			slices.Reverse(accumulatedCallstack)
			return accumulatedCallstack, len(accumulatedCallstack) > 0
		}
		accumulatedCallstack = append(accumulatedCallstack, frame)
	}
	if !capturing {
		panic(ErrNoCaptureFrame)
	}
	return nil, false
}

func isExecuteReplayCallFlowFrame(frame runtime.Frame) bool {
	file, line := executeReplayCallFlowLocation()
	return frame.File == file && frame.Line == line
}
