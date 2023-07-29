package execution_scheduler

import "testing"

type simpleHandlers struct {
	t                   *testing.T
	maxCount            int
	handlerCount        int
	errorHandlerCount   int
	handlerChannel      chan bool
	errorHandlerChannel chan bool
}

func newSimpleHandlers(t *testing.T, maxCount int) *simpleHandlers {
	return &simpleHandlers{
		t:                   t,
		handlerCount:        0,
		errorHandlerCount:   0,
		maxCount:            maxCount,
		handlerChannel:      make(chan bool),
		errorHandlerChannel: make(chan bool),
	}
}

func (pair *simpleHandlers) handler() error {
	if pair.handlerCount > pair.maxCount {
		pair.t.Fatalf("simple handler should not be called more than %d", pair.maxCount)
	}
	pair.handlerCount += 1
	pair.handlerChannel <- true
	return nil
}

func (pair *simpleHandlers) errorHandler(err error) error {
	if pair.errorHandlerCount > pair.maxCount {
		pair.t.Fatalf("simple handler error should not be called more than %d", pair.maxCount)
	}
	pair.errorHandlerCount += 1
	pair.errorHandlerChannel <- true
	return err
}

func (pair *simpleHandlers) wait() {
	select {
	case <-pair.handlerChannel:
	case <-pair.errorHandlerChannel:
	}
}
