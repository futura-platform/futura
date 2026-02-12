package futura_test

import (
	"testing"

	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
)

func TestNewPlainDurableHandle(t *testing.T) {
	type plainState struct {
		Visible string
		hidden  string
		Count   int
	}

	t.Run("persists and restores plain values including unexported fields", func(t *testing.T) {
		constructorCalls := 0
		handle := futura.NewPlainDurableHandle("plainHandle",
			func() *plainState {
				constructorCalls++
				return &plainState{
					Visible: "from-constructor",
					hidden:  "from-constructor-hidden",
					Count:   1,
				}
			},
		)

		flowFn := func(b futura.FlowBuilder, _ struct{}) (plainState, error) {
			b = handle.Provide(b)
			ref, persist := handle.Use(b)
			if ref.Visible == "from-constructor" {
				ref.Visible = "persisted"
				ref.hidden = "persisted-hidden"
				ref.Count = 2
				didChange := persist()
				assert.True(t, didChange)
			}
			return *ref, nil
		}

		container := executiontype.NewInMemoryContainer()

		r, err := futura.NewFlowFromContainer[struct{}, plainState](container).Execute(
			t.Context(),
			flowFn,
			struct{}{},
		)
		assert.NoError(t, err)
		assert.Equal(t, plainState{
			Visible: "persisted",
			hidden:  "persisted-hidden",
			Count:   2,
		}, r)
		assert.Equal(t, 1, constructorCalls)

		r, err = futura.NewFlowFromContainer[struct{}, plainState](container).Execute(
			t.Context(),
			flowFn,
			struct{}{},
		)
		assert.NoError(t, err)
		assert.Equal(t, plainState{
			Visible: "persisted",
			hidden:  "persisted-hidden",
			Count:   2,
		}, r)
		assert.Equal(t, 1, constructorCalls)
	})
}
