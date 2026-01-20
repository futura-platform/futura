package utils_test

import (
	"bytes"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/futura/internal/utils"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
)

func TestSerializableSet(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	encoder := privateencoding.NewEncoder[utils.SerializableSet[int]](buf)

	set := utils.NewSerializableSet(mapset.NewSet(1, 2, 3))
	err := encoder.Encode(set)
	assert.NoError(t, err)

	decoder := privateencoding.NewDecoder[utils.SerializableSet[int]](buf)
	decodedSet, err := decoder.Decode()
	assert.NoError(t, err)
	assert.Equal(t, set, decodedSet)
}
