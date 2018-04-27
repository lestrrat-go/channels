package fanin

import (
	"context"
	"log"
	"reflect"

	"github.com/pkg/errors"
)

func Start(ctx context.Context, ch interface{}) (*RemoteControl, error) {
	chrv := reflect.ValueOf(ch)
	if !chrv.IsValid() || chrv.Kind() != reflect.Chan {
		typ := "nil"
		if chrv.IsValid() {
			typ = chrv.Type().String()
		}
		return nil, errors.Errorf(`a valid channel must be passed to fanin.Start (got %s)`, typ)
	}

	if chrv.Kind() != reflect.Chan {
		return nil, errors.Errorf(`valid channel is required (%s)`, chrv.Kind())
	}

	if chrv.Type().ChanDir()&reflect.SendDir == 0 {
		return nil, errors.Errorf(`channel must allows us to write to it`)
	}

	addCh := make(chan reflect.Value)
	m := &minion{
		addCh: addCh,
		dst:   chrv,
	}

	go m.receive(ctx)

	ctrl := &RemoteControl{
		addCh: addCh,
	}
	return ctrl, nil
}

func (c *RemoteControl) Add(ctx context.Context, ch interface{}) error {
	chrv := reflect.ValueOf(ch)
	if !chrv.IsValid() || chrv.Kind() != reflect.Chan {
		typ := "nil"
		if chrv.IsValid() {
			typ = chrv.Type().String()
		}
		return errors.Errorf(`a valid channel must be passed to fanin.Add (got %s)`, typ)
	}

	if chrv.Kind() != reflect.Chan {
		return errors.Errorf(`valid channel is required (%s)`, chrv.Kind())
	}

	if chrv.Type().ChanDir()&reflect.RecvDir == 0 {
		return errors.Errorf(`channel must allows us to read from it`)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case c.addCh <- chrv:
	}
	return nil
}

func (m *minion) receive(ctx context.Context) error {
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

	var chans []reflect.Value
OUTER:
	for {
		cases = cases[:receiveCaseMax]

		for _, src := range chans {
			cases = append(cases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: src,
			})
		}

		for {
			chosen, recv, recvOK := reflect.Select(cases)

			switch chosen {
			case receiveCaseDone:
				return ctx.Err()
			case receiveCaseAdd:
				if !recvOK {
					// If the add channel is broken ... well... I guess we don't have to
					// finish the loop
					continue
				}

				chans = append(chans, recv.Interface().(reflect.Value))
				continue OUTER
			case receiveCaseRemove:
				if !recvOK {
					// If the add channel is broken ... well... I guess we don't have to
					// finish the loop
					continue
				}

				for i, ch := range chans {
					if ch == recv {
						chans = append(chans[:i], chans[i+1:]...)
						continue OUTER
					}
				}
			default:
				if !recvOK {
					for i, ch := range chans {
						if ch == recv {
							chans = append(chans[:i], chans[i+1:]...)
							continue OUTER
						}
					}
					continue OUTER
				}
				m.dst.Send(recv)
			}
		}
	}
}
