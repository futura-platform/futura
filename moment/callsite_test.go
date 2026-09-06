package moment_test

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/futura-platform/futura/moment"
)

func TestCallpathString(t *testing.T) {
	callpath := moment.Callpath{{File: "file1", Line: 1}, {File: "file2", Line: 2}}
	assert.Equal(t, "file1:1 -> \nfile2:2", callpath.String())
}
