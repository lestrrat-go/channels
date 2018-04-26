package fanout

import (
	"reflect"
	"sync"
)

const (
	receiveCaseDone = iota
	receiveCaseAdd
	receiveCaseRemove
	receiveCaseData
	receiveCaseMax
)

const (
	fanoutCaseDone = iota
	fanoutCaseRemove
	fanoutCaseStaticMax
)

// Control is a structure that remote controls the fanout pattern.
// It is resopnsible for providing end users the ability to register
// and deregister channels.
//
// Stopping the fanout is not part of RemoteControl's API, as this
// can be controlled via the context.Context object passed to fanout.Start()
type RemoteControl struct {
	addCh    chan interface{}
	removeCh chan interface{}
	chType   reflect.Type
}

// minion is the real work horse behind this pattern, but is not exposed
// to the end user. it controls two loops: a reader, which waits for the
// user to send us something, and the writer, which does the actually
// fanning out to the destinations
type minion struct {
	addCh           chan interface{}
	cond            *sync.Cond
	dst             []reflect.Value // slice of wrapped channels
	inSelect        bool
	muDestination   sync.RWMutex
	muInSelect      sync.RWMutex
	muPending       sync.RWMutex
	notifyRemovalCh chan interface{}
	pending         []reflect.Value
	removeCh        chan interface{}
	src             reflect.Value // wrapped channel
}
