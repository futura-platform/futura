package replay

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/futura-platform/futura/internal/flow/moment"
	"github.com/futura-platform/futura/internal/utils/testutil"
	"github.com/stretchr/testify/assert"
)

var thisFile string

func init() {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to get caller")
	}
	thisFile = file
}

func TestGetClosestReplayExecutionFrame(t *testing.T) {
	assert.Equal(t, filepath.Base(thisFile), "getClosestReplayUserCallpath_test.go")
	t.Run("returns the closest replay execution callsite frame", func(t *testing.T) {
		t.Run("single parent, direct child", func(t *testing.T) {
			Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
				func() {
					callpath, ok := GetClosestReplayUserCallpath(0)
					assert.True(t, ok)
					assert.Len(t, callpath, 1)
					assert.Equal(t, callpath[0].File, thisFile)
					assert.Equal(t, callpath[0].Line, 36)
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
						callpath, ok := GetClosestReplayUserCallpath(0)
						assert.True(t, ok)
						assert.Len(t, callpath, recurses)
						assert.Equal(t, callpath[0].File, thisFile)
						assert.Equal(t, callpath[0].Line, 57)
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
							callpath, ok := GetClosestReplayUserCallpath(0)
							assert.True(t, ok)
							assert.Len(t, callpath, 1)
							assert.Equal(t, callpath[0].File, thisFile)
							assert.Equal(t, callpath[0].Line, 75)
						}()
						return nil, nil
					}, nil)
				}, nil)
			}, nil)
		})
	})
	t.Run("multiple layers of user abstraction between the replay execution callsite and the GetClosestReplayUserCallpath call, including a futura frame", func(t *testing.T) {
		someUserAbstractionA := func() {
			callpath, ok := GetClosestReplayUserCallpath(0)
			assert.True(t, ok)
			assert.Equal(t, moment.Callpath{
				{File: thisFile, Line: 102},
				{File: thisFile, Line: 98},
				{File: thisFile, Line: 94},
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
		_, ok := GetClosestReplayUserCallpath(0)
		assert.False(t, ok)
	})
}
