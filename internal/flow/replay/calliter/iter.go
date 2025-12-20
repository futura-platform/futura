package calliter

import (
	"iter"
	"runtime"
)

// NewFrameIter returns an iterator over the current goroutine's call stack.
//
// The returned iterator conforms to the standard library's iter.Seq type and
// can be used with a range loop:
//
//	iter := NewFrameIter(16, 0)
//	for frame := range iter {
//		// use frame
//	}
//
// initialPCBufSize controls the initial size of the []uintptr buffer passed to
// runtime.Callers. If the buffer is too small to hold the complete call stack,
// the implementation will grow it until the full stack has been captured.
//
// The skip parameter has the same meaning as the skip parameter passed
// directly to runtime.Callers.
func NewFrameIter(initialPCBufSize, skip int) iter.Seq[runtime.Frame] {
	return func(yield func(runtime.Frame) bool) {
		// Grow the PC buffer until it is large enough to hold the complete
		// call stack starting at the requested skip.
		pc := make([]uintptr, initialPCBufSize)
		baseSkip := skip + 3
		n := runtime.Callers(baseSkip, pc)
		for n == len(pc) {
			pc = make([]uintptr, len(pc)*2)
			n = runtime.Callers(baseSkip, pc)
		}

		frames := runtime.CallersFrames(pc[:n])
		for {
			frame, more := frames.Next()
			if !more {
				return
			}
			if !yield(frame) {
				return
			}
		}
	}
}
