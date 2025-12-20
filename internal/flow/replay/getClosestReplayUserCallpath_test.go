package replay

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetClosestReplayExecutionFrame(t *testing.T) {
	t.Run("returns the closest replay execution callsite frame", func(t *testing.T) {
		t.Run("single parent, direct child", func(t *testing.T) {
			Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
				func() {
					callpath, ok := GetClosestReplayUserCallpath(0)
					assert.True(t, ok)
					assert.Len(t, callpath, 1)
					assert.Equal(t, filepath.Base(callpath[0].File), "getClosestReplayUserCallpath_test.go")
					assert.Equal(t, callpath[0].Line, 22)
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
						assert.Equal(t, filepath.Base(callpath[0].File), "getClosestReplayUserCallpath_test.go")
						assert.Equal(t, callpath[0].Line, 43)
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
							assert.Equal(t, filepath.Base(callpath[0].File), "getClosestReplayUserCallpath_test.go")
							assert.Equal(t, callpath[0].Line, 61)
						}()
						return nil, nil
					}, nil)
				}, nil)
			}, nil)
		})
	})
	t.Run("multiple layers of user abstraction between the replay execution callsite and the GetClosestReplayUserCallpath call", func(t *testing.T) {
		someUserAbstractionA := func() {
			callpath, ok := GetClosestReplayUserCallpath(0)
			assert.True(t, ok)
			assert.Len(t, callpath, 3)
		}

		someUserAbstractionB := func() {
			someUserAbstractionA()
		}

		someUserAbstractionC := func() {
			someUserAbstractionB()
		}

		Execute(t.Context(), func(ctx context.Context, args any) (any, error) {
			someUserAbstractionC()
			return nil, nil
		}, nil)
	})
	t.Run("returns false if there is no parent replay execution frame", func(t *testing.T) {
		_, ok := GetClosestReplayUserCallpath(0)
		assert.False(t, ok)
	})
}
