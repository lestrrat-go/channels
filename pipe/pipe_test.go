package pipe_test

import (
	"context"
	"testing"
	"time"

	"github.com/lestrrat-go/channels/pipe"
	"github.com/stretchr/testify/assert"
)

func TestPipe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	src := make(chan int)
	dst := make(chan int, 10)
	if !assert.NoError(t, pipe.Pipe(ctx, dst, src, nil), `pipe.Pipe should suceed`) {
		return
	}

	for i := 0; i < 10; i++ {
		src <- i
	}

	var values []int
	for v := range dst {
		values = append(values, v)
	}

	if !assert.Equal(t, values, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, `values should match`) {
		return
	}
}
