package moment_test

import (
	"testing"

	"github.com/futura-platform/futura/moment"
)

// this just exists to cover the code with tests, and to ensure the String method exists.
// This doesn't actually require the test to run though, since the compiler will complain if the method is not present.
func TestCallpathString(t *testing.T) {
	callpath := moment.Callpath{{File: "file1", Line: 1}, {File: "file2", Line: 2}}
	callpath.String()
}
