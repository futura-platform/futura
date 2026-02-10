package privateencoding_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodec_IgnoresNoCopyStructures(t *testing.T) {
	t.Run("sync.Mutex", func(t *testing.T) {
		type WithMutex struct {
			Mu sync.Mutex
			N  int
		}

		unlocked := &WithMutex{N: 1}

		locked := &WithMutex{N: 1}
		locked.Mu.Lock()
		lockedBytes := encodeValue(t, locked) // encode while locked
		locked.Mu.Unlock()

		unlockedBytes := encodeValue(t, unlocked)

		// If privateencoding ignores nocopy structures like sync.Mutex,
		// the serialized bytes should not depend on the mutex state.
		assert.Equal(t, unlockedBytes, lockedBytes)
	})
}
