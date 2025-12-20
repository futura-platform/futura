package ftrerrors

import (
	"errors"
	"fmt"
)

var ErrInconsistentState = errors.New("inconsistent state")

func InconsistentStateError(subErr error) error {
	return fmt.Errorf("%w: %w", ErrInconsistentState, subErr)
}
