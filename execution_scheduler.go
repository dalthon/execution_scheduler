package execution_scheduler

import (
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
)

type ExecutionKind uint64

const (
	Parallel ExecutionKind = iota
	Serial
	Critical
)

type SchedulerStatus uint64

const (
	PendingStatus SchedulerStatus = iota
	ActiveStatus
	InactiveStatus
	ClosingStatus
	ClosedStatus
	ErrorStatus
)

type ExecutionEvent uint64

const (
	PreparedEvent ExecutionEvent = iota
	ScheduledEvent
	FinishedEvent
	WakedEvent
	ClosingEvent
	ErrorEvent
)

type schedulerInterface interface {
	getLock() *sync.Mutex
	getClock() clockwork.Clock
	signal(event ExecutionEvent)
	remove(execution *Execution)
}

type Scheduler struct {
	waitGroup       *sync.WaitGroup
	lock            sync.Mutex
	parallelRunning uint32
	status          SchedulerStatus
	parallelQueue   *ExecutionQueue
	serialQueue     *ExecutionQueue
	events          chan ExecutionEvent
	clock           clockwork.Clock
}

func NewScheduler(waitGroup *sync.WaitGroup) *Scheduler {
	scheduler := &Scheduler{
		parallelRunning: 0,
		waitGroup:       waitGroup,
		status:          PendingStatus,
		parallelQueue:   NewExecutionQueue(),
		serialQueue:     NewExecutionQueue(),
		events:          make(chan ExecutionEvent),
		clock:           clockwork.NewRealClock(),
	}
	waitGroup.Add(1)
	go scheduler.eventLoop()

	return scheduler
}

func (scheduler *Scheduler) Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	var execution *Execution
	if kind == Parallel {
		execution = scheduler.parallelQueue.Push(
			handler,
			errorHandler,
			kind,
			priority,
		)
	} else {
		execution = scheduler.serialQueue.Push(
			handler,
			errorHandler,
			kind,
			priority,
		)
	}

	execution.setExpiration(scheduler, time.Duration(2)*time.Second)

	scheduler.signal(ScheduledEvent)
}

func (scheduler *Scheduler) Run() {
	go scheduler.runPrepareCallback()
}

func (scheduler *Scheduler) eventLoop() {
	for {
		switch <-scheduler.events {
		case PreparedEvent:
			fmt.Println("Received PreparedEvent")
			scheduler.setStatus(ActiveStatus)
		case ScheduledEvent:
			fmt.Println("Received ScheduledEvent")
			switch scheduler.status {
			case InactiveStatus:
				scheduler.setStatus(ActiveStatus)
			case ActiveStatus:
				scheduler.execute()
			}
		case FinishedEvent:
			fmt.Println("Received FinishedEvent")
			scheduler.parallelRunning -= 1
			if scheduler.status == ActiveStatus || scheduler.status == InactiveStatus {
				if scheduler.isRunning() || scheduler.isScheduled() {
					scheduler.execute()
				} else {
					scheduler.setStatus(InactiveStatus)
				}
			}
		case WakedEvent:
			fmt.Println("Received WakedEvent")
			if scheduler.isScheduled() {
				scheduler.setStatus(ActiveStatus)
			} else {
				scheduler.setStatus(ClosingStatus)
			}
		case ClosingEvent:
			fmt.Println("Received ClosingEvent")
			if scheduler.isScheduled() {
				scheduler.setStatus(ActiveStatus)
			} else {
				scheduler.setStatus(ClosedStatus)
			}
		case ErrorEvent:
			fmt.Println("Received ErrorEvent")
			scheduler.setStatus(ErrorStatus)
		}
	}
}

func (scheduler *Scheduler) setStatus(status SchedulerStatus) {
	if scheduler.status == status {
		return
	}

	scheduler.status = status
	switch scheduler.status {
	case PendingStatus:
		fmt.Println("status: Pending")
		go scheduler.runPrepareCallback()
	case ActiveStatus:
		fmt.Println("status: Active")
		scheduler.execute()
	case InactiveStatus:
		fmt.Println("status: Inactive")
		go scheduler.runOnInactive()
	case ClosingStatus:
		fmt.Println("status: Closing")
		go scheduler.runOnClosingCallback()
	case ClosedStatus:
		fmt.Println("status: Closed")
		go scheduler.runOnCloseCallback()
	case ErrorStatus:
		fmt.Println("status: Error")
		go scheduler.runOnErrorCallback()
	}
}

// TODO: Handle execution logic properly
func (scheduler *Scheduler) execute() {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	for execution := scheduler.parallelQueue.Pop(); execution != nil; execution = scheduler.parallelQueue.Pop() {
		scheduler.parallelRunning += 1
		go execution.call(scheduler)
		return
	}

	for execution := scheduler.serialQueue.Pop(); execution != nil; execution = scheduler.serialQueue.Pop() {
		scheduler.parallelRunning += 1
		go execution.call(scheduler)
		return
	}
}

func (scheduler *Scheduler) getLock() *sync.Mutex {
	return &scheduler.lock
}

func (scheduler *Scheduler) getClock() clockwork.Clock {
	return scheduler.clock
}

func (scheduler *Scheduler) signal(event ExecutionEvent) {
	scheduler.events <- event
}

func (scheduler *Scheduler) remove(execution *Execution) {
	if execution.kind == Parallel {
		scheduler.parallelQueue.Remove(execution)
	} else {
		scheduler.serialQueue.Remove(execution)
	}
}

// TODO: Handle serial
func (scheduler *Scheduler) isRunning() bool {
	return scheduler.parallelRunning > 0
}

func (scheduler *Scheduler) isScheduled() bool {
	return scheduler.serialQueue.Size() > 0 || scheduler.parallelQueue.Size() > 0
}

// TODO: add prepare callback, run it and then use it here
func (scheduler *Scheduler) runPrepareCallback() {
	scheduler.signal(PreparedEvent)
}

// TODO: add inactive callback, run it and then use it here
func (scheduler *Scheduler) runOnInactive() {
	scheduler.signal(WakedEvent)
}

// TODO: add closing callback, run it and then use it here
func (scheduler *Scheduler) runOnClosingCallback() {
	scheduler.signal(ClosingEvent)
}

// TODO: add close callback, run it and then use it here
func (scheduler *Scheduler) runOnCloseCallback() {
	scheduler.waitGroup.Done()
}

func (scheduler *Scheduler) runOnErrorCallback() {

}
