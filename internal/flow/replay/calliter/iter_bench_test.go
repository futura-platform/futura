package calliter_test

import (
	"strconv"
	"testing"

	"github.com/futura-platform/futura/internal/flow/replay/calliter"
)

// consume the first n frames, the way GetClosestReplayUserCallstack stops at the replay frame
func consume(n int) int {
	got := 0
	for range calliter.NewFrameIter(8, 0) {
		got++
		if got == n {
			break
		}
	}
	return got
}

//go:noinline
func deep(depth int, f func() int) int {
	if depth == 0 {
		return f()
	}
	return deep(depth-1, f)
}

func BenchmarkFrameIter(b *testing.B) {
	for _, depth := range []int{0, 100, 1000} {
		b.Run("depth="+strconv.Itoa(depth), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				deep(depth, func() int { return consume(3) })
			}
		})
	}
}
