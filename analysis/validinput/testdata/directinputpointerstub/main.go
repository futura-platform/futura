package main

import (
	"context"

	"github.com/futura-platform/futura"
)

func stepWithPointerInput(ctx context.Context, args *int) (int, error) {
	return *args, nil
}

func run() (int, error) {
	f := futura.NewFlow[struct{}, int]()
	return f.Execute(context.Background(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
		x := 42
		return futura.Step(b, stepWithPointerInput, &x)
	}, struct{}{})
}
