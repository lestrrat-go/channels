package pipe

import "reflect"

const (
	receiveCaseDone = iota
	receiveCaseSrc
	receiveCaseMax
)

type FilterFunc func(reflect.Value) (reflect.Value, bool)
