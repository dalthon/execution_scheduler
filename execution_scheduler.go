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
	OnErrorFinishedEvent
	OnCrashFinishedEvent
	RefreshEvent
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
	executionTimeout time.Duration
	inactivityDelay  time.Duration
	onPrepare        func(scheduler *Scheduler) error
	onClosing        func(scheduler *Scheduler) error
	onLeaveError     func(scheduler *Scheduler) error
	onError          func(scheduler *Scheduler) error
	onCrash          func(scheduler *Scheduler)
	onClose          func(scheduler *Scheduler)
}

type Scheduler struct {
	Status          SchedulerStatus
	Err             error
	options         *SchedulerOptions
	lock            sync.Mutex
	parallelRunning uint32
	parallelQueue   *ExecutionQueue
	serialQueue     *ExecutionQueue
	events          chan ExecutionEvent
	callbackRunning bool
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
		callbackRunning: false,
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

// TODO: validate status denying schedule on Crashed and Closed statuses
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

		if scheduler.options.executionTimeout != time.Duration(0) {
			execution.setExpiration(
				scheduler,
				scheduler.options.executionTimeout,
				func() {
					scheduler.lock.Lock()
					scheduler.parallelRunning += 1
					defer scheduler.lock.Unlock()
				},
			)
		}
	} else {
		execution = scheduler.serialQueue.Push(
			handler,
			errorHandler,
			kind,
			priority,
		)

		if scheduler.options.executionTimeout != time.Duration(0) {
			execution.setExpiration(
				scheduler,
				scheduler.options.executionTimeout,
				func() {
					scheduler.lock.Lock()
					scheduler.parallelRunning += 1
					defer scheduler.lock.Unlock()
				},
			)
		}
	}
	scheduler.signal(ScheduledEvent)

	return execution
}

// TODO: think about not having it
func (scheduler *Scheduler) Run() {
	scheduler.runPrepareCallback()
}

// TODO: test and think about ForceClose() corner cases
func (scheduler *Scheduler) ForceClose() {
	scheduler.signal(ClosingEvent)
}

func (scheduler *Scheduler) eventLoop() {
	for {
		event := <-scheduler.events
		scheduler.lock.Lock()
		switch event {
		case RefreshEvent:
			scheduler.callbackRunning = false
			scheduler.setStatus(PendingStatus)
		case PreparedEvent:
			scheduler.setStatus(ActiveStatus)
		case ScheduledEvent:
			switch scheduler.Status {
			case InactiveStatus:
				scheduler.setStatus(ActiveStatus)
			case ActiveStatus:
				scheduler.execute()
			case CrashedStatus:
				go scheduler.cancelExecutions()
			case ClosedStatus:
				go scheduler.cancelExecutions()
			}
		case FinishedEvent: // TODO: rethink about how to behave on FinishedEvent
			scheduler.parallelRunning -= 1
			switch scheduler.Status {
			case ActiveStatus:
				if scheduler.isScheduled() {
					scheduler.execute()
				} else {
					if !scheduler.isRunning() {
						scheduler.setStatus(InactiveStatus)
					}
				}
			case ErrorStatus:
				if !scheduler.callbackRunning && !scheduler.isRunning() {
					scheduler.runOnLeaveErrorCallback()
				}
			case CrashedStatus:
				if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
					scheduler.setStatus(ClosedStatus)
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
			scheduler.parallelRunning -= 1
			switch scheduler.Status {
			case ErrorStatus:
				if !scheduler.callbackRunning && !scheduler.isRunning() {
					scheduler.runOnLeaveErrorCallback()
				}
			case CrashedStatus:
				if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
					scheduler.setStatus(ClosedStatus)
				}
			default:
				scheduler.setStatus(ErrorStatus)
			}
		case OnErrorFinishedEvent:
			scheduler.callbackRunning = false
			if !scheduler.isRunning() {
				scheduler.runOnLeaveErrorCallback()
			}
		case OnCrashFinishedEvent:
			scheduler.callbackRunning = false
			if !scheduler.isRunning() && !scheduler.isScheduled() {
				scheduler.setStatus(ClosedStatus)
			}
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
	}

	scheduler.Status = status
	switch scheduler.Status {
	case PendingStatus:
		scheduler.runOnPrepare()
	case ActiveStatus:
		scheduler.execute()
	case InactiveStatus:
		scheduler.runOnInactive()
	case ClosingStatus:
		scheduler.runOnClosingCallback()
	case ClosedStatus:
		scheduler.runOnClose()
	case ErrorStatus:
		scheduler.runOnErrorCallback()
	case CrashedStatus:
		scheduler.runOnCrash()
	}
}

// TODO: handle serial execution
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

// TODO: handle serial execution
func (scheduler *Scheduler) cancelExecutions() {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	err := NewSchedulerCrashedError()

	for execution := scheduler.parallelQueue.Pop(); execution != nil; execution = scheduler.parallelQueue.Pop() {
		scheduler.parallelRunning += 1
		go execution.expire(scheduler, err)
	}

	for execution := scheduler.serialQueue.Pop(); execution != nil; execution = scheduler.serialQueue.Pop() {
		scheduler.parallelRunning += 1
		go execution.expire(scheduler, err)
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

// TODO: handle serial execution
func (scheduler *Scheduler) isRunning() bool {
	return scheduler.parallelRunning > 0
}

func (scheduler *Scheduler) isScheduled() bool {
	return scheduler.serialQueue.Size() > 0 || scheduler.parallelQueue.Size() > 0
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

func (scheduler *Scheduler) runOnClose() {
	if scheduler.waitGroup != nil {
		scheduler.waitGroup.Done()
	}

	if scheduler.options.onClose != nil {
		go scheduler.options.onClose(scheduler)
	}
}

func (scheduler *Scheduler) runOnPrepare() {
	scheduler.Err = nil
	scheduler.runPrepareCallback()
}

// TODO: add tests to onCrash callback
// TODO: should close only after all executions are finished
// TODO: should not go to closing, but to close
func (scheduler *Scheduler) runOnCrash() {
	go scheduler.cancelExecutions()

	if scheduler.options.onCrash == nil {
		scheduler.signal(OnCrashFinishedEvent)
		return
	}

	scheduler.callbackRunning = true
	go func() {
		scheduler.options.onCrash(scheduler)
		scheduler.signal(OnCrashFinishedEvent)
	}()
}

// TODO: add tests to onLeaveError callback
func (scheduler *Scheduler) runOnLeaveErrorCallback() {
	scheduler.callbackRunning = true
	if scheduler.Err != nil {
		scheduler.signal(CrashedEvent)
		return
	}

	if scheduler.options.onLeaveError == nil {
		scheduler.Err = NewSchedulerNotRecovered()
		scheduler.signal(CrashedEvent)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(scheduler.options.onLeaveError, RefreshEvent)
}

func (scheduler *Scheduler) runOnErrorCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(scheduler.options.onError, OnErrorFinishedEvent)
}

func (scheduler *Scheduler) runPrepareCallback() {
	scheduler.runCallbackAndFireEvent(scheduler.options.onPrepare, PreparedEvent)
}

func (scheduler *Scheduler) runOnClosingCallback() {
	scheduler.runCallbackAndFireEvent(scheduler.options.onClosing, ClosingEvent)
}

func (scheduler *Scheduler) runCallbackAndFireEvent(callback func(*Scheduler) error, event ExecutionEvent) {
	if scheduler.Err != nil {
		return
	}

	if callback == nil {
		scheduler.signal(event)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(callback, event)
}

func (scheduler *Scheduler) runAsyncCallbackAndFireEvent(callback func(*Scheduler) error, event ExecutionEvent) {
	if scheduler.Err != nil {
		return
	}

	err := callback(scheduler)
	if err == nil {
		scheduler.signal(event)
		return
	}

	scheduler.Err = err
	scheduler.signal(CrashedEvent)
}
