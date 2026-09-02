package replay

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

var (
	thisFile            string
	transparentCallFile string
)

func init() {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to get caller")
	}
	thisFile = file
	transparentCallFile = filepath.Join(filepath.Dir(filepath.Dir(filepath.Dir(file))), "utils", "testutil", "transparentCall.go")

	// this package has no capture function of its own, so the tests pin one here.
	SetCaptureFunction(capture)
}

// capture stands in for the pinned capture function: frames at or below it are excluded.
func capture() ([]runtime.Frame, bool) {
	return GetClosestReplayUserCallstack()
}

func TestGetClosestReplayExecutionFrame(t *testing.T) {
	assert.Equal(t, filepath.Base(thisFile), "getClosestReplayUserCallstack_test.go")
	t.Run("returns the closest replay execution callsite frame", func(t *testing.T) {
		t.Run("single parent, direct child", func(t *testing.T) {
			Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
				func() {
					callstack, ok := capture()
					callpath := CallstackToCallpath(callstack)
					assert.True(t, ok)
					assert.Equal(t, moment.Callpath{
						{File: thisFile, Line: 50},
						{File: thisFile, Line: 43},
					}, callpath)
				}()
				return nil, nil
			}, nil)
		})
		testWithResurseCount := func(t *testing.T, recurses int) {
			t.Run(fmt.Sprintf("indirect child, %d recurses", recurses), func(t *testing.T) {
				Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
					var indirect func()
					recursesLeft := recurses
					indirect = func() {
						recursesLeft--
						if recursesLeft > 0 {
							indirect()
							return
						}
						callstack, ok := capture()
						callpath := CallstackToCallpath(callstack)
						assert.True(t, ok)
						// the recursion frames, plus the frame that calls capture
						assert.Len(t, callpath, recurses+1)
						assert.Equal(t, moment.Callsite{File: thisFile, Line: 73}, callpath[0])
						assert.Equal(t, moment.Callsite{File: thisFile, Line: 65}, callpath[len(callpath)-1])
					}
					indirect()
					return nil, nil
				}, nil)
			})
		}
		testWithResurseCount(t, 10)
		testWithResurseCount(t, 1000)

		t.Run("multiple parents, direct child", func(t *testing.T) {
			Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
				return Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
					return Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
						func() {
							callstack, ok := capture()
							callpath := CallstackToCallpath(callstack)
							assert.True(t, ok)
							assert.Equal(t, moment.Callpath{
								{File: thisFile, Line: 93},
								{File: thisFile, Line: 86},
							}, callpath)
						}()
						return nil, nil
					}, nil)
				}, nil)
			}, nil)
		})
	})
	t.Run("records every frame above the capture function, including futura's own helpers", func(t *testing.T) {
		someUserAbstractionA := func() {
			callstack, ok := capture()
			callpath := CallstackToCallpath(callstack)
			assert.True(t, ok)
			assert.Equal(t, moment.Callpath{
				{File: thisFile, Line: 125},
				{File: transparentCallFile, Line: 10},
				{File: thisFile, Line: 121},
				{File: transparentCallFile, Line: 10},
				{File: thisFile, Line: 117},
				{File: transparentCallFile, Line: 10},
				{File: thisFile, Line: 102},
			}, callpath)
		}

		someUserAbstractionB := func() {
			testutil.TransparentCall(someUserAbstractionA)
		}

		someUserAbstractionC := func() {
			testutil.TransparentCall(someUserAbstractionB)
		}

		Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
			testutil.TransparentCall(someUserAbstractionC)
			return nil, nil
		}, nil)
	})
	t.Run("returns false if there is no parent replay execution frame", func(t *testing.T) {
		_, ok := capture()
		assert.False(t, ok)
	})
	t.Run("panics if the callstack is captured outside of the capture function", func(t *testing.T) {
		testutil.PanicsWithErrorIs(t, ErrNoCaptureFrame, func() {
			Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
				GetClosestReplayUserCallstack()
				return nil, nil
			}, nil)
		})
	})
}
