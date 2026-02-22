package privateencoding

import "errors"

var (
	errInterfaceTypeNotRegistered = errors.New("interface type not registered")
	errInterfaceTypeMismatch      = errors.New("decoded type is not assignable to interface")
)
