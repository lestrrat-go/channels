package fanout_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lestrrat-go/channels/fanout"
	"github.com/stretchr/testify/assert"
)

func TestConstructor(t *testing.T) {
	t.Run("Invalid arguments", func(t *testing.T) {
		data := []interface{}{
			nil,
			[]string{},
			make(chan<- struct{}),
		}

		for _, arg := range data {
			rc, err := fanout.Start(context.Background(), arg)
			if !assert.Error(t, err, `fanout.Start should fail`) {
				return
			}
			if !assert.Empty(t, rc, `fanout.Start should not return a valid RemoteControl object`) {
				return
			}
		}
	})
}

func TestFanoutCloseSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch := make(chan int)

	rc, err := fanout.Start(ctx, ch)
	if !assert.NoError(t, err, `fanout.Start should succeed`) {
		return
	}

	time.AfterFunc(10*time.Millisecond, func() {
		close(ch)
	})

	select {
	case <-ctx.Done():
		t.Errorf(`context canceled before detecting workers exited: %s`, ctx.Err())
	case <-rc.Done():
		if !assert.Error(t, rc.Err(), `err should not be empty`) {
			return
		}
	}
}

func TestFanout(t *testing.T) { 
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch := make(chan int)

	rc, err := fanout.Start(ctx, ch)
	if !assert.NoError(t, err, `fanout.Start should succeed`) {
		return
	}

	count := 10
	done := make(chan struct{})
	chans := make([]chan int, count)
	messages := []int{5, 3, 1, 2, 4, 8, 9, 7, 6, 10}
	var wg sync.WaitGroup

	receiver := func(ctx context.Context, wg *sync.WaitGroup, id int, ch chan int) {
		defer wg.Done()
		var values []int
		var loop = true
		for loop {
			select {
			case <-ctx.Done():
				loop = false
			case v, ok := <-ch:
				if !ok {
					loop = false
				}
				values = append(values, v)

				if len(values) == count {
					loop = false
				}
			}
		}

		assert.Equal(t, messages, values, `messages should match values`)
	}

	for i := 0; i < count; i++ {
	t.Logf("Adding chan %d", i)
		ch := make(chan int)
		chans[i] = ch
		if !assert.NoError(t, rc.Add(ctx, ch), `rc.Add should succeed`) {
			return
		}
		wg.Add(1)
		go receiver(ctx, &wg, i, ch)
	}

	// Wait till everybody is done
	go func(done chan struct{}, wg *sync.WaitGroup) {
		wg.Wait()
		close(done)
	}(done, &wg)

	for _, v := range messages {
		select {
		case ch <- v:
		case <-time.After(time.Second):
			t.Errorf(`timed out waiting to write`)
		}
	}

	select {
	case <-ctx.Done():
		t.Errorf(`context canceled before detecting workers exited: %s`, ctx.Err())
	case <-rc.Done():
		// bailed out of loop somehow?
		t.Errorf(`unexpected end of fanout: %s`, rc.Err())
	case <-done:
	}
}
