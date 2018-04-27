package fanout

import (
	"context"
	"reflect"
	"sync"

	"github.com/lestrrat-go/channels/pipe"
	"github.com/pkg/errors"
)

// Start takes an arbitrary channel (`src`) and returns a handle that
// allows you to register more channels to receive data that was obtained
// from reading from `src`. Each of the registered channels receive the
// same data sent to `src`, in the order that was obtained.
//
// The behavior is a specialized case of fanout pattern, where the
// fanout mechanism only acts as a multiplexer that sends identical
// data to the recipients. In real life "fanout", you typicaly divide
// tasks to the awaiting channels/queues such that you cna efficiently
// a large task that has been divided into smaller components.
// (Note: this beavior can be mimiced using this simple multiplexer
// fanout as well)
func Start(ctx context.Context, src interface{}) (*RemoteControl, error) {
	chrv := reflect.ValueOf(src)
	if !chrv.IsValid() || chrv.Kind() != reflect.Chan {
		typ := "nil"
		if chrv.IsValid() {
			typ = chrv.Type().String()
		}
		return nil, errors.Errorf(`a valid channel must be passed to fanout.New (got %s)`, typ)
	}

	if (chrv.Type().ChanDir() & reflect.RecvDir) == 0 {
		return nil, errors.New(`channel passed to fanout.New must be able to received from`)
	}

	var m minion
	m.addCh = make(chan interface{})
	m.removeCh = make(chan interface{})
	m.cond = sync.NewCond(&sync.Mutex{})

	errCh := make(chan error, 1)

	go func() {
		errCh <- m.receive(ctx, chrv)
	}()

	go func() {
		errCh <- m.fanout(ctx)
	}()

	doneCh := make(chan struct{})
	ctrl := &RemoteControl{
		chType:   chrv.Type().Elem(), // Used to validate arguments to Add()
		addCh:    m.addCh,            // Used to send channels to be added
		removeCh: m.removeCh,         // Used to send channels to be removed
		errCh:    errCh,              // Used to check if we have any errors
		doneCh:   doneCh,             // Used to check if the fanout goroutines have stopped
	}

	pipe.Pipe(ctx, doneCh, errCh, func(rv reflect.Value) (reflect.Value, bool) {
		ctrl.mu.Lock()
		if ctrl.err == nil {
			ctrl.err = rv.Interface().(error)
			close(doneCh)
		}
		ctrl.mu.Unlock()
		return rv, false
	})

	return ctrl, nil
}

func (c *RemoteControl) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}

func (c *RemoteControl) Done() <-chan struct{} {
	return c.doneCh
}

func (m *minion) receive(ctx context.Context, src reflect.Value) error {
	// We can't create a real select {} statement here, because
	// we don't know the channel element type until runtime.
	cases := make([]reflect.SelectCase, receiveCaseMax)
	cases[receiveCaseDone] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}
	cases[receiveCaseAdd] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(m.addCh),
	}
	cases[receiveCaseRemove] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(m.removeCh),
	}
	cases[receiveCaseData] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: src,
	}

	for {
		chosen, v, recvOK := reflect.Select(cases)
		switch chosen {
		case receiveCaseDone:
			return ctx.Err()
		case receiveCaseAdd:
			m.muDestination.Lock()
			// v is a reflect.Value(interface{}(chan))
			m.dst = append(m.dst, v.Elem())
			m.muDestination.Unlock()
		case receiveCaseRemove:
			m.muDestination.Lock()
			// v is a reflect.Value(interface{}(chan))
			ch := v.Elem()
			for i, e := range m.dst {
				if e == ch {
					m.dst = append(m.dst[:i], m.dst[i+1:]...)
					break
				}
			}
			m.muDestination.Unlock()

			m.muInSelect.RLock()
			if m.inSelect {
				m.notifyRemovalCh <- ch
			}
			m.muInSelect.RUnlock()
		case receiveCaseData:
			if !recvOK {
				return errors.New(`source channel closed`)
			}
			m.muPending.Lock()
			m.pending = append(m.pending, v)
			m.muPending.Unlock()

			// wake up the sender
			m.cond.Broadcast()
		}
	}
}

func (m *minion) pendingAvailable(threshold int) bool {
	m.muPending.RLock()
	defer m.muPending.RUnlock()

	l := len(m.pending)
	return l > threshold
}

// waitPending is stolen from lestrrat-go/fluent-client. Oh wait, I wrote it...
func (m *minion) waitPending(ctx context.Context) error {
	// We need to check for ctx.Done() here before getting into
	// the cond loop, because otherwise we might never be woken
	// up again
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	for {
		if m.pendingAvailable(0) {
			break
		}

		select {
		case <-ctx.Done():
			return nil
		default:
		}

		m.cond.Wait()
	}
	return nil
}

// Add adds a channel that receives the data. Data that is sent to the
// channel must be read at the same pace as it is written to the source.
// If not, writes may actually block or the in-memory buffer could grow
// *VERY* large.
//
// If you think you may need to work with bursts, try adding a buffer to
// the channels that you pass to this method.
func (c *RemoteControl) Add(ctx context.Context, ch interface{}) error {
	chrv := reflect.ValueOf(ch)
	if !chrv.IsValid() || chrv.Kind() != reflect.Chan {
		typ := "nil"
		if chrv.IsValid() {
			typ = chrv.Type().String()
		}
		return errors.Errorf(`a valid channel must be passed to fanout.Add (got %s)`, typ)
	}

	if (chrv.Type().ChanDir() & reflect.SendDir) == 0 {
		return errors.New(`channel passed to fanout.Add must be able to sent to`)
	}

	if c.chType != chrv.Type().Elem() {
		return errors.Errorf(`channel element type must be %s, but got %s`, c.chType, chrv.Type())
	}

	// Notify that we want to add this
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.addCh <- chrv.Interface():
	}

	return nil
}

func (c *RemoteControl) Remove(ctx context.Context, ch interface{}) error {
	// Notify that we want to remove this
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.removeCh <- reflect.ValueOf(ch):
	}
	return nil
}

func (m *minion) fanout(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Wait until there's something to be done
		if err := m.waitPending(ctx); err != nil {
			return errors.Wrap(err, `failed while waiting for pending data`)
		}

		// Keep popping until we have nothing to process
		for m.pendingAvailable(0) {
			m.muPending.Lock()
			v := m.pending[0]
			m.pending = m.pending[1:]
			m.muPending.Unlock()

			m.sendValue(ctx, v)
		}
	}
	return nil
}

func (m *minion) sendValue(ctx context.Context, v reflect.Value) error {
	m.muDestination.RLock()
	cases := make([]reflect.SelectCase, len(m.dst)+fanoutCaseStaticMax)
	chans := make([]reflect.Value, len(m.dst))
	copy(chans, m.dst)
	m.muDestination.RUnlock()

	for len(chans) > 0 {
		// "clear" the slice
		cases = cases[:fanoutCaseStaticMax]

		// First one is always the ctx.Done() channels
		cases[fanoutCaseDone] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		}

		// Second one is the request to remove a channel. This is only
		// important when we are attempting to send to the (possibly)
		// defunct channel, so we need to include it in this select
		cases[fanoutCaseRemove] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(m.notifyRemovalCh),
		}

		// Create select cases to send the value to. Only one of these
		// will actually be selected, so we splice the one that got
		// the chance to be sent at the end of this loop
		for _, ch := range chans {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Chan: ch, // ch is already wrapped in reflect.ValueOf
				Send: v,
			})
		}

		m.muInSelect.Lock()
		m.inSelect = true
		m.muInSelect.Unlock()
		chosen, recv, recvOK := reflect.Select(cases)

		// Note: we *might* have received removal notice right here
		m.muInSelect.Lock()
		m.inSelect = true
		m.muInSelect.Unlock()

		// drain removal notification. be careful not to block
		for loop := true; loop; {
			select {
			case removeCh, ok := <-m.notifyRemovalCh:
				if ok {
					chrv := reflect.ValueOf(removeCh)
					for i, ch := range chans {
						if ch == chrv {
							chans = append(chans[:i], chans[i+1:]...)
							break
						}
					}
				}
			default:
				loop = false
			}
		}

		switch chosen {
		case fanoutCaseDone:
			// We're done
			return ctx.Err()
		case fanoutCaseRemove:
			// we have a problem. if recvOK == false
			if recvOK {
				// We got a request to remove a channel
				// we have to make sure to remove it from our current list,
				// and retry the above select
				for i, ch := range chans {
					if ch == recv {
						chans = append(chans[:i], chans[i+1:]...)
						break
					}
				}
			}
			continue
		default:
			// "splice" the chans slice so the next "send" would
			// not send to the same channel again
			chans = append(chans[:chosen-fanoutCaseStaticMax], chans[chosen+1-fanoutCaseStaticMax:]...)
		}
	}

	return nil
}
