package calliter

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFrameIter(t *testing.T) {
	t.Run("0 skip starts on the parent callsite", func(t *testing.T) {
		toCall := func() {
			iter := NewFrameIter(8, 0)
			for frame := range iter {
				assert.Equal(t, filepath.Base(frame.File), "iter_test.go")
				assert.Equal(t, frame.Line, 21)
				break
			}
		}
		toCall()
	})
	t.Run("matches runtime.Callers(i)", func(t *testing.T) {
		matchesRuntimeCallers(t, 8)
	})
	t.Run("matches runtime.Callers(i) with large initial buffer size", func(t *testing.T) {
		matchesRuntimeCallers(t, 1024)
	})
	t.Run("matches runtime.Callers(i) with small initial buffer size", func(t *testing.T) {
		matchesRuntimeCallers(t, 1)
	})
	t.Run("matches runtime.Callers(i) with large call stack depth", func(t *testing.T) {
		recurseCount := 1000
		var recurseThenTest func(n int)
		recurseThenTest = func(n int) {
			if n == 0 {
				matchesRuntimeCallers(t, 8)
				return
			}
			recurseThenTest(n - 1)
		}
		recurseThenTest(recurseCount)
	})
}

func inlineMe(i int) int { return i + 1 }

//go:noinline
func viaInline(n int, f func()) {
	if n == 0 {
		f()
		return
	}
	viaInline(inlineMe(n)-2, f)
}

func TestFrameIter_ChunkBoundariesUnderInlining(t *testing.T) {
	// an inlined call is one PC that expands to several frames, so a chunk boundary must never fall
	// inside one; every chunk size is checked frame by frame against runtime.Caller
	for _, size := range []int{1, 2, 3, 5, 8} {
		viaInline(20, func() {
			baseSkip := 1
			i := 0
			for frame := range NewFrameIter(size, baseSkip) {
				_, file, line, ok := runtime.Caller(baseSkip + i + 3)
				if !ok {
					break
				}
				assert.Equal(t, line, frame.Line, "size=%d frame %d", size, i)
				assert.Equal(t, file, frame.File, "size=%d frame %d", size, i)
				i++
			}
			assert.Greater(t, i, 20, "size=%d", size)
		})
	}
}

func matchesRuntimeCallers(t *testing.T, initialBufferSize int) {
	baseSkip := 2
	iter := NewFrameIter(initialBufferSize, baseSkip)
	i := 0
	for frame := range iter {
		pc, file, line, ok := runtime.Caller(baseSkip + i + 3)
		i++
		assert.True(t, ok)
		assert.Equal(t, pc, frame.PC)
		assert.Equal(t, file, frame.File)
		assert.Equal(t, line, frame.Line)
	}
}
