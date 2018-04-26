package fanout_test

import (
	"context"
	"fmt"
	"time"

	"github.com/lestrrat-go/channels/fanout"
)

func Example() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a ticker
	src := time.NewTicker(100 * time.Millisecond)
	defer src.Stop()

	// Start fanout of the values generated from the ticker
	rc, err := fanout.Start(ctx, src.C)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	// Start 10 goroutines that should receive everything that we send
	// to it
	for i := 0; i < 10; i++ {
		ch := make(chan time.Time)
		if err := rc.Add(ctx, ch); err != nil {
			fmt.Println(err.Error())
			return
		}

		go func(ctx context.Context, ch chan time.Time) {
			for {
				select {
				case <-ctx.Done():
					return
				case t := <-ch:
					fmt.Println(t.Format(time.RFC3339))
				}
			}
		}(ctx, ch)
	}

	<-ctx.Done()
}
