package privateencoding

import "fmt"

func pathError(action, path string, err error) error {
	return fmt.Errorf("failed to %s %s: %w", action, path, err)
}
