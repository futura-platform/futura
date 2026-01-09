package ftype

import "errors"

var (
	// Return this error from a flow fn to immediately return the error from the loop.
	ErrCancelFlow = errors.New("flow cancelled")
)
