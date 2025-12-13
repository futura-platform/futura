package futura

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithFlow(t *testing.T) {
	ctx := context.Background()
	ctx = withFlow(ctx)
	flowContext, ok := getFlowContext(ctx)
	assert.True(t, ok)
	assert.NotNil(t, flowContext)

	ctx2 := context.Background()
	flowContext2, ok := getFlowContext(ctx2)
	assert.False(t, ok)
	assert.Nil(t, flowContext2)
}
