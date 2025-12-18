package ftype

type FlowLoopHooks struct {
	OnError []func(err error) (continueExecution bool)
}

type FlowLoopOptions struct {
	Hooks FlowLoopHooks
}

type FlowLoopOption func(*FlowLoopOptions)

func WithOnError(onError func(err error) (continueExecution bool)) FlowLoopOption {
	return func(opts *FlowLoopOptions) {
		opts.Hooks.OnError = append(opts.Hooks.OnError, onError)
	}
}
