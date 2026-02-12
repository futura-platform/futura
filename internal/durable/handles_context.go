package durable

import (
	"context"
	"errors"

	"github.com/futura-platform/futura/ftype"
	"github.com/puzpuzpuz/xsync/v4"
)

type handlesContextKey string

const handlesKey handlesContextKey = "durable_handles"

var (
	ErrHandlesAlreadyExists = errors.New("handles already exists")
)

type HandleKey string

func WithHandlesCache() ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		_, alreadyExists := GetHandles(ctx)
		if alreadyExists {
			panic(ErrHandlesAlreadyExists)
		}

		return context.WithValue(ctx, handlesKey, xsync.NewMap[HandleKey, any]())
	}
}

func GetHandles(ctx context.Context) (*xsync.Map[HandleKey, any], bool) {
	m, ok := ctx.Value(handlesKey).(*xsync.Map[HandleKey, any])
	return m, ok
}
