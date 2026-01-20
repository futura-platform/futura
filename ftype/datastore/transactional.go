package datastore

import "context"

// Transactional is a transactional storage interface.
// It allows for the storage of data to be transactional,
// meaning that all changes applied to "tx" should be committed to the storage when the function returns without error.
type Transactional[W, R any] interface {
	Transact(ctx context.Context, fn func(ctx context.Context, tx W) error) error
	ReadTransact(ctx context.Context, fn func(ctx context.Context, tx R) error) error
}
