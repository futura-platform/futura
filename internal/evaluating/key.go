// Package evaluating owns the context key under which the step evaluator attaches the
// identity of the moment being evaluated. It is deliberately typeless: the evaluator writes
// a moment.Identity under Key, and moment reads it back, without either depending on this
// package for anything but the key.
package evaluating

type key struct{}

// Key is the context key for the currently evaluating moment's identity.
var Key key
