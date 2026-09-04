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
		// The stack is fetched a chunk at a time, and the next chunk only if the consumer wants more
		// frames: a consumer that stops early never pays for the frames below its stopping point.
		// A chunk is refetched from the top with a larger buffer if it was filled, so that an inlined
		// call, which occupies one PC but expands to several frames, is never split across chunks.
		pc := make([]uintptr, initialPCBufSize)
		baseSkip := skip + 3
		for {
			n := runtime.Callers(baseSkip, pc)
			if n == 0 {
				return
			}
			// the last PC of a full buffer may expand to more frames than fit, and a partial buffer
			// holds the rest of the stack, so only a full buffer's last PC is held back
			frames := runtime.CallersFrames(pc[:n])
			held := n == len(pc)
			yielded := 0
			for {
				frame, more := frames.Next()
				if held && !more {
					// the held-back tail is re-fetched with the next chunk
					break
				}
				if !yield(frame) {
					return
				}
				yielded++
				if !more {
					return
				}
			}
			// the next chunk starts where this one's yielded frames end
			baseSkip += yielded
			pc = make([]uintptr, len(pc)*2)
		}
	}
}
