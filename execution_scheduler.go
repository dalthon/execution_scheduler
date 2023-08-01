package execution_scheduler

import (
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
	Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution
}

type SchedulerOptions struct {
	inactivityDelay time.Duration
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
	options         *SchedulerOptions
	inactivityTimer clockwork.Timer
}

func NewScheduler(options *SchedulerOptions, waitGroup *sync.WaitGroup) *Scheduler {
	scheduler := &Scheduler{
		parallelRunning: 0,
		waitGroup:       waitGroup,
		status:          PendingStatus,
		parallelQueue:   NewExecutionQueue(),
		serialQueue:     NewExecutionQueue(),
		events:          make(chan ExecutionEvent, 16),
		clock:           clockwork.NewRealClock(),
		options:         options,
		inactivityTimer: nil,
	}
	if waitGroup != nil {
		waitGroup.Add(1)
	}
	go scheduler.eventLoop()

	return scheduler
}

func (scheduler *Scheduler) Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
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

	scheduler.signal(ScheduledEvent)

	return execution
}

// TODO: Think about not having it
func (scheduler *Scheduler) Run() {
	go scheduler.runPrepareCallback()
}

// TODO: scheduler.ForceClose()
func (scheduler *Scheduler) ForceClose() {
	if scheduler.waitGroup != nil {
		scheduler.waitGroup.Done()
	}
}

func (scheduler *Scheduler) eventLoop() {
	for {
		switch <-scheduler.events {
		case PreparedEvent:
			scheduler.setStatus(ActiveStatus)
		case ScheduledEvent:
			switch scheduler.status {
			case InactiveStatus:
				scheduler.setStatus(ActiveStatus)
			case ActiveStatus:
				scheduler.execute()
			}
		case FinishedEvent:
			scheduler.parallelRunning -= 1
			if scheduler.status == ActiveStatus || scheduler.status == InactiveStatus {
				if scheduler.isRunning() || scheduler.isScheduled() {
					scheduler.execute()
				} else {
					scheduler.setStatus(InactiveStatus)
				}
			}
		case WakedEvent:
			if scheduler.isScheduled() {
				scheduler.setStatus(ActiveStatus)
			} else {
				scheduler.setStatus(ClosingStatus)
			}
		case ClosingEvent:
			if scheduler.isScheduled() {
				scheduler.setStatus(ActiveStatus)
			} else {
				scheduler.setStatus(ClosedStatus)
			}
		case ErrorEvent:
			scheduler.setStatus(ErrorStatus)
		}
	}
}

func (scheduler *Scheduler) setStatus(status SchedulerStatus) {
	if scheduler.status == status {
		return
	}

	switch scheduler.status {
	case InactiveStatus:
		scheduler.lock.Lock()
		if scheduler.inactivityTimer != nil {
			scheduler.inactivityTimer.Stop()
			scheduler.inactivityTimer = nil
		}
		scheduler.lock.Unlock()
	}

	scheduler.status = status
	switch scheduler.status {
	case PendingStatus:
		go scheduler.runPrepareCallback()
	case ActiveStatus:
		scheduler.execute()
	case InactiveStatus:
		scheduler.runOnInactive()
	case ClosingStatus:
		go scheduler.runOnClosingCallback()
	case ClosedStatus:
		go scheduler.runOnCloseCallback()
	case ErrorStatus:
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

func (scheduler *Scheduler) runOnInactive() {
	if scheduler.options.inactivityDelay == time.Duration(0) {
		scheduler.signal(WakedEvent)
		return
	}

	scheduler.lock.Lock()
	scheduler.inactivityTimer = scheduler.clock.AfterFunc(
		scheduler.options.inactivityDelay,
		scheduler.wakeFromInactivity,
	)
	scheduler.lock.Unlock()
}

func (scheduler *Scheduler) wakeFromInactivity() {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if scheduler.inactivityTimer != nil {
		scheduler.inactivityTimer.Stop()
		scheduler.inactivityTimer = nil
	}

	scheduler.signal(WakedEvent)
}

// TODO: add closing callback, run it and then use it here
func (scheduler *Scheduler) runOnClosingCallback() {
	scheduler.signal(ClosingEvent)
}

// TODO: add close callback, run it and then use it here
func (scheduler *Scheduler) runOnCloseCallback() {
	if scheduler.waitGroup != nil {
		scheduler.waitGroup.Done()
	}
}

func (scheduler *Scheduler) runOnErrorCallback() {
}
