package ftype

import (
	"context"
)

type FlowLoopOption func(context.Context) context.Context
