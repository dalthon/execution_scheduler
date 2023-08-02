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
	CrashedStatus
)

type ExecutionEvent uint64

const (
	PreparedEvent ExecutionEvent = iota
	ScheduledEvent
	FinishedEvent
	WakedEvent
	ClosingEvent
	ErrorEvent
	CrashedEvent
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
	onPrepare       func(scheduler *Scheduler) error
}

type Scheduler struct {
	Status          SchedulerStatus
	options         *SchedulerOptions
	lock            sync.Mutex
	parallelRunning uint32
	parallelQueue   *ExecutionQueue
	serialQueue     *ExecutionQueue
	events          chan ExecutionEvent
	clock           clockwork.Clock
	waitGroup       *sync.WaitGroup
	inactivityTimer clockwork.Timer
}

func NewScheduler(options *SchedulerOptions, waitGroup *sync.WaitGroup) *Scheduler {
	scheduler := &Scheduler{
		Status:          PendingStatus,
		options:         options,
		parallelRunning: 0,
		parallelQueue:   NewExecutionQueue(),
		serialQueue:     NewExecutionQueue(),
		events:          make(chan ExecutionEvent, 16),
		clock:           clockwork.NewRealClock(),
		waitGroup:       waitGroup,
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
	scheduler.runPrepareCallback()
}

// TODO: scheduler.ForceClose()
func (scheduler *Scheduler) ForceClose() {
	if scheduler.waitGroup != nil {
		scheduler.waitGroup.Done()
	}
}

func (scheduler *Scheduler) eventLoop() {
	for {
		event := <-scheduler.events
		scheduler.lock.Lock()
		switch event {
		case PreparedEvent:
			scheduler.setStatus(ActiveStatus)
		case ScheduledEvent:
			switch scheduler.Status {
			case InactiveStatus:
				scheduler.setStatus(ActiveStatus)
			case ActiveStatus:
				scheduler.execute()
			}
		case FinishedEvent:
			scheduler.parallelRunning -= 1
			if scheduler.Status == ActiveStatus || scheduler.Status == InactiveStatus {
				if scheduler.isRunning() || scheduler.isScheduled() {
					scheduler.execute()
				} else {
					scheduler.setStatus(InactiveStatus)
				}
			}
		case WakedEvent:
			if scheduler.isRunning() || scheduler.isScheduled() {
				scheduler.setStatus(ActiveStatus)
			} else {
				scheduler.setStatus(ClosingStatus)
			}
		case ClosingEvent:
			if scheduler.isRunning() || scheduler.isScheduled() {
				scheduler.setStatus(ActiveStatus)
			} else {
				scheduler.setStatus(ClosedStatus)
			}
		case ErrorEvent:
			scheduler.setStatus(ErrorStatus)
		case CrashedEvent:
			scheduler.setStatus(CrashedStatus)
		}
		scheduler.lock.Unlock()
	}
}

func (scheduler *Scheduler) setStatus(status SchedulerStatus) {
	if scheduler.Status == status {
		return
	}

	switch scheduler.Status {
	case InactiveStatus:
		if scheduler.inactivityTimer != nil {
			scheduler.inactivityTimer.Stop()
			scheduler.inactivityTimer = nil
		}
	case ErrorStatus:
		scheduler.runOnLeaveErrorCallback()
	}

	scheduler.Status = status
	switch scheduler.Status {
	case PendingStatus:
		scheduler.runPrepareCallback()
	case ActiveStatus:
		scheduler.execute()
	case InactiveStatus:
		scheduler.runOnInactive()
	case ClosingStatus:
		scheduler.runOnClosingCallback()
	case ClosedStatus:
		scheduler.runOnCloseCallback()
	case ErrorStatus:
		scheduler.runOnErrorCallback()
	case CrashedStatus:
		scheduler.runOnCrashedCallback()
	}
}

// TODO: Handle execution logic properly
func (scheduler *Scheduler) execute() {
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

// TODO: If onPrepare hook fails, what should we do?
func (scheduler *Scheduler) runPrepareCallback() {
	if scheduler.options.onPrepare == nil {
		scheduler.signal(PreparedEvent)
		return
	}

	go func() {
		err := scheduler.options.onPrepare(scheduler)
		if err == nil {
			scheduler.signal(PreparedEvent)
			return
		}

		scheduler.signal(CrashedEvent)
	}()
}

func (scheduler *Scheduler) runOnInactive() {
	if scheduler.options.inactivityDelay == time.Duration(0) {
		scheduler.signal(WakedEvent)
		return
	}

	scheduler.inactivityTimer = scheduler.clock.AfterFunc(
		scheduler.options.inactivityDelay,
		scheduler.wakeFromInactivity,
	)
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

// TODO: add error callback, run it and then use it here
func (scheduler *Scheduler) runOnErrorCallback() {
}

// TODO: add leave error callback, run it and then use it here
func (scheduler *Scheduler) runOnLeaveErrorCallback() {
}

// TODO: add crashed callback (or use close?), run it and then use it here
// TODO: once it is crashed, we shlould error all executions
func (scheduler *Scheduler) runOnCrashedCallback() {
	if scheduler.waitGroup != nil {
		scheduler.waitGroup.Done()
	}
}
