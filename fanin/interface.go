package fanin

import "reflect"

const (
	receiveCaseDone = iota
	receiveCaseAdd
	receiveCaseRemove
	receiveCaseMax
)

type RemoteControl struct {
	addCh    chan<- reflect.Value
	removeCh chan<- reflect.Value
}

type minion struct {
	addCh    <-chan reflect.Value
	dst      reflect.Value // channel to write to
	removeCh <-chan reflect.Value
}
