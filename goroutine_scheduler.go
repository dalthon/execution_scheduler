package goroutine_scheduler

import (
	"fmt"
	"sync"
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

type Scheduler struct {
	waitGroup       *sync.WaitGroup
	lock            sync.Mutex
	parallelRunning uint32
	status          SchedulerStatus
	parallelQueue   *ExecutionQueue
	serialQueue     *ExecutionQueue
	events          chan ExecutionEvent
}

func NewScheduler(waitGroup *sync.WaitGroup) *Scheduler {
	scheduler := &Scheduler{
		parallelRunning: 0,
		waitGroup:       waitGroup,
		status:          PendingStatus,
		parallelQueue:   NewExecutionQueue(),
		serialQueue:     NewExecutionQueue(),
		events:          make(chan ExecutionEvent),
	}
	waitGroup.Add(1)
	go scheduler.eventLoop()

	return scheduler
}

func (scheduler *Scheduler) Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if kind == Parallel {
		scheduler.parallelQueue.Push(
			handler,
			errorHandler,
			kind,
			priority,
		)
	} else {
		scheduler.serialQueue.Push(
			handler,
			errorHandler,
			kind,
			priority,
		)
	}

	scheduler.events <- ScheduledEvent
}

func (scheduler *Scheduler) Run() {
	go scheduler.runPrepareCallback()
}

func (scheduler *Scheduler) eventLoop() {
	for {
		fmt.Println("Event!")
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
	}

	for execution := scheduler.serialQueue.Pop(); execution != nil; execution = scheduler.serialQueue.Pop() {
		scheduler.parallelRunning += 1
		go execution.call(scheduler)
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
	scheduler.events <- PreparedEvent
}

// TODO: add inactive callback, run it and then use it here
func (scheduler *Scheduler) runOnInactive() {
	scheduler.events <- WakedEvent
}

// TODO: add closing callback, run it and then use it here
func (scheduler *Scheduler) runOnClosingCallback() {
	scheduler.events <- ClosingEvent
}

// TODO: add close callback, run it and then use it here
func (scheduler *Scheduler) runOnCloseCallback() {
	scheduler.waitGroup.Done()
}

func (scheduler *Scheduler) runOnErrorCallback() {

}
