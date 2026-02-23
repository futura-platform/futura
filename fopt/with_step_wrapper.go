package fopt

import (
	"context"

	"github.com/futura-platform/futura/ftype"
	stepwrapper "github.com/futura-platform/futura/internal/step/wrapper"
)

func WithStepWrapper(wrapper stepwrapper.StepWrapper) ftype.FlowLoopOption {
	return func(ctx context.Context) context.Context {
		return stepwrapper.With(ctx, wrapper)
	}
}
