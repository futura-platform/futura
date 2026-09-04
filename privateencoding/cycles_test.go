package privateencoding_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
)

type cyclicNode struct {
	V    int
	Next *cyclicNode
}

type withEmbeddedMutex struct {
	sync.Mutex
	N int
}

type lockable struct{ ID int }

func (lockable) Lock()   {}
func (lockable) Unlock() {}

func TestEncoder_CyclicValue(t *testing.T) {
	t.Run("a self-referential pointer is an error, not an overflow", func(t *testing.T) {
		n := &cyclicNode{V: 1}
		n.Next = n
		var buf bytes.Buffer
		err := privateencoding.NewEncoder[*cyclicNode](&buf).Encode(n)
		assert.ErrorIs(t, err, privateencoding.ErrCyclicValue)
	})
	t.Run("a shared pointee that is not a cycle encodes", func(t *testing.T) {
		shared := &cyclicNode{V: 1}
		pair := struct{ A, B *cyclicNode }{shared, shared}
		var buf bytes.Buffer
		assert.NoError(t, privateencoding.NewEncoder[struct{ A, B *cyclicNode }](&buf).Encode(pair))
	})
}

func TestEncoder_OnlySyncTypesAreSkipped(t *testing.T) {
	t.Run("a struct embedding a mutex encodes its own fields", func(t *testing.T) {
		a := encodeValue(t, &withEmbeddedMutex{N: 1})
		b := encodeValue(t, &withEmbeddedMutex{N: 2})
		assert.NotEqual(t, a, b)
		decoded, err := privateencoding.NewDecoder[*withEmbeddedMutex](bytes.NewReader(a)).Decode()
		assert.NoError(t, err)
		assert.Equal(t, 1, decoded.N)
	})
	t.Run("a user type with Lock and Unlock methods is not a lock", func(t *testing.T) {
		assert.NotEqual(t, encodeValue(t, &lockable{1}), encodeValue(t, &lockable{2}))
	})
}

type firstFieldAlias struct {
	Backing [4]int
	View    []int
	Self    *[4]int
}

func TestEncoder_AliasOfOwnFirstFieldIsNotACycle(t *testing.T) {
	// a struct and its first field share an address, so a pointer or slice into the first field looks
	// like the enclosing pointer if only the address is compared
	v := &firstFieldAlias{Backing: [4]int{1, 2, 3, 4}}
	v.View = v.Backing[:2]
	v.Self = &v.Backing
	decoded := roundTrip(t, v)
	assert.Equal(t, v.Backing, decoded.Backing)
	assert.Equal(t, []int{1, 2}, decoded.View)
	assert.Equal(t, &v.Backing, decoded.Self)
}
