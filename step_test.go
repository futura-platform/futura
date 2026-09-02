package futura_test

import (
	"context"
	"errors"
	"testing"

	"github.com/futura-platform/futura"
	"github.com/stretchr/testify/assert"
)

func TestStep(t *testing.T) {
	t.Run("a step re-executed with a new input memoizes that input, not the original one", func(t *testing.T) {
		replays := 0
		var outputs []int
		var executedWith []int
		_, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			input := 1
			if replays == 2 {
				input = 2
			}
			out, err := futura.Step(b, func(ctx context.Context, in int) (int, error) {
				executedWith = append(executedWith, in)
				return in * 10, nil
			}, input)
			if err != nil {
				return 0, err
			}
			outputs = append(outputs, out)
			if replays < 3 {
				return 0, futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			}
			return out, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, []int{10, 20, 10}, outputs)
		assert.Equal(t, []int{1, 2, 1}, executedWith)
	})
	t.Run("a step re-executed with a new input hits the memo when the input repeats", func(t *testing.T) {
		// The input goes 1 -> 2 -> 2. The second replay with 2 must be a memo hit.
		replays := 0
		var executedWith []int
		_, err := futura.NewFlow[struct{}, int]().Execute(t.Context(), func(b futura.FlowBuilder, _ struct{}) (int, error) {
			replays++
			input := 1
			if replays >= 2 {
				input = 2
			}
			out, err := futura.Step(b, func(ctx context.Context, in int) (int, error) {
				executedWith = append(executedWith, in)
				return in * 10, nil
			}, input)
			if err != nil {
				return 0, err
			}
			if replays < 3 {
				return 0, futura.Action(b, func(ctx context.Context) error { return errors.New("retry") })
			}
			return out, nil
		}, struct{}{})
		assert.NoError(t, err)
		assert.Equal(t, []int{1, 2}, executedWith)
	})
}
