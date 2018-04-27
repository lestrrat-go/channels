package pipe

import (
	"context"
	"reflect"

	"github.com/pkg/errors"
)

// Pipe creates a chain of channels, with an optional filter.
// dst and src must be channels, where dst can be written to,
// and src can be read from. They do not have to both support
// the same element types, but if they do not match, and the
// filter below does not do any translation, then the values
// will be discarded
//
// f is an optional filter, which receives the data read from
// src as reflect.Value. This function should return the value
// to be delegated to dst as reflect.Value. If the second return
// value is false, then the value is discarded, and nothing will
// be written to dst.
//
// Data received from src will be written immediately to dst, without
// any internal buffering. This means that without an explicit buffer
// on the channels, data may block if nobody is reading.
//
// dst will be closed upon completion of the Pipe.
func Pipe(ctx context.Context, dst, src interface{}, f FilterFunc) error {
	var dstrv reflect.Value
	var srcrv reflect.Value
	var err error

	if dstrv, err = chanRV(dst); err != nil {
		return errors.Wrap(err, `invalid dst`)
	}

	if srcrv, err = chanRV(src); err != nil {
		return errors.Wrap(err, `invalid src`)
	}

	// Make sure the channels have the correct send/recv direction
	if dstrv.Type().ChanDir()&reflect.SendDir == 0 {
		return errors.Errorf(`dst channel must allow sending data to it`)
	}
	if srcrv.Type().ChanDir()&reflect.RecvDir == 0 {
		return errors.Errorf(`dst channel must allow receiving data from it`)
	}

	go pipe(ctx, dstrv, srcrv, f)
	return nil
}

var zeroval reflect.Value

func chanRV(ch interface{}) (reflect.Value, error) {
	if ch == nil {
		return zeroval, errors.New(`channel is nil`)
	}

	chrv := reflect.ValueOf(ch)
	if chrv.Kind() != reflect.Chan {
		return zeroval, errors.Errorf(`value is not a valid channel (%s)`, chrv.Type())
	}

	return chrv, nil
}

func pipe(ctx context.Context, dst, src reflect.Value, f FilterFunc) error {
	// when we bail out of this loop, we need to close the channel
	defer dst.Close()

	cases := make([]reflect.SelectCase, receiveCaseMax)
	cases[receiveCaseDone] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	}
	cases[receiveCaseSrc] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: src,
	}

	for {
		chosen, recv, recvOK := reflect.Select(cases)

		switch chosen {
		case receiveCaseDone:
			return ctx.Err()
		case receiveCaseSrc:
			if !recvOK {
				return errors.New(`src channel closed`)
			}

			if f != nil {
				var ok bool
				recv, ok = f(recv)
				if !ok {
					continue
				}
			}

			if dst.Type().Elem() == recv.Type() {
				dst.Send(recv)
			}

		}
	}
	return nil
}
