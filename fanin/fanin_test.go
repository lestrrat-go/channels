package fanin_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/channels/fanin"
	"github.com/stretchr/testify/assert"
)

func TestFanin(t *testing.T) {
	const count = 10

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dst := make(chan int, count)
	rc, err := fanin.Start(ctx, dst)
	if !assert.NoError(t, err, `fanin.Start should succeed`) {
		return
	}

	// Create N goroutines that write an int to a channel
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		ch := make(chan int)
		if !assert.NoError(t, rc.Add(ctx, ch), `rc.Add should succeed`) {
			return
		}

		go func(ctx context.Context, v int, ch chan int) {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case ch <- v:
			}
		}(ctx, i, ch)
	}

	wg.Wait()

	time.Sleep(500*time.Millisecond)

	if !assert.Len(t, dst, count, "we should have %d items", count) {
		return
	}

}
