package execution_scheduler

import (
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

type mockedScheduler struct {
	lock              sync.Mutex
	events            []ExecutionEvent
	removedExecutions []*Execution
	clock             clockwork.Clock
}

func newMockedScheduler() *mockedScheduler {
	return &mockedScheduler{
		clock: clockwork.NewFakeClock(),
	}
}

func (scheduler *mockedScheduler) getLock() *sync.Mutex {
	return &scheduler.lock
}

func (scheduler *mockedScheduler) getClock() clockwork.Clock {
	return scheduler.clock
}

func (scheduler *mockedScheduler) signal(event ExecutionEvent) {
	scheduler.events = append(scheduler.events, event)
}

func (scheduler *mockedScheduler) remove(execution *Execution) {
	scheduler.removedExecutions = append(scheduler.removedExecutions, execution)
}

func (scheduler *mockedScheduler) advance(duration time.Duration) {
	scheduler.clock.(clockwork.FakeClock).Advance(duration)
}
