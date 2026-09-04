package ftrerrors_test

import (
	"errors"
	"testing"

	"github.com/futura-platform/futura/internal/errors"
	"github.com/stretchr/testify/assert"
)

func TestInconsistentStateError(t *testing.T) {
	err := ftrerrors.InconsistentStateError(errors.New("sub error"))
	assert.ErrorIs(t, err, ftrerrors.ErrInconsistentState)
}
