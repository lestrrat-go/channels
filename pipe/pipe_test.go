package pipe_test

import (
	"context"
	"testing"
	"time"

	"github.com/lestrrat-go/channels/pipe"
	"github.com/stretchr/testify/assert"
)

func TestPipe(t *testing.T) {
	var data = []int{5, 9, 6, 3, 4, 2, 1, 8, 0, 7}

	t.Run("Normal Send", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		src := make(chan int)
		dst := make(chan int, 10)
		if !assert.NoError(t, pipe.Pipe(ctx, dst, src, nil), `pipe.Pipe should suceed`) {
			return
		}

		for _, v := range data {
			src <- v
		}

		var values []int
		for v := range dst {
			values = append(values, v)
		}

		if !assert.Equal(t, values, data, `values should match`) {
			return
		}
	})
	t.Run("Varying channel types", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		src := make(chan int)
		dst := make(chan struct{}, 10)
		if !assert.NoError(t, pipe.Pipe(ctx, dst, src, nil), `pipe.Pipe should suceed`) {
			return
		}

		for _, v := range data {
			src <- v
		}

		var values []struct{}
		for v := range dst {
			values = append(values, v)
		}

		if !assert.Empty(t, values, data, `values should be empty`) {
			return
		}
	})
}
