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
	ShutdownStatus
)

type ExecutionEvent uint64

const callbackEventsMask = 0x100

const finishMasks = 0xE00
const parallelFinishMask = 0x200
const serialCalledFinishMask = 0x400
const serialExpiredFinishMask = 0x800

const (
	ScheduledEvent ExecutionEvent = iota
	WakedEvent
	CrashedEvent
	ShutdownEvent

	PreparedEvent ExecutionEvent = callbackEventsMask | iota
	RefreshEvent
	ClosingEvent
	OnErrorFinishedEvent
	OnCrashFinishedEvent

	FinishedParallelEvent ExecutionEvent = parallelFinishMask | iota
	ErrorParallelEvent

	FinishedSerialEvent ExecutionEvent = serialCalledFinishMask | iota
	ErrorSerialEvent

	ExpiredFinishedSerialEvent ExecutionEvent = serialExpiredFinishMask | iota
	ExpiredSerialEvent
)

type schedulerInterface interface {
	beforeExecutionCall(execution *Execution)
	beforeExpireCall(execution *Execution)
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
	serialRunning   uint32
	currentSerial   *Execution
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
		serialRunning:   0,
		currentSerial:   nil,
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

	if scheduler.options.executionTimeout != time.Duration(0) {
		execution.setExpiration(scheduler, scheduler.options.executionTimeout)
	}
	scheduler.signal(ScheduledEvent)

	return execution
}

func (scheduler *Scheduler) Run() {
	scheduler.runPrepareCallback()
}

func (scheduler *Scheduler) Shutdown() {
	scheduler.signal(ShutdownEvent)
}

func (scheduler *Scheduler) eventLoop() {
	for {
		event := <-scheduler.events
		scheduler.lock.Lock()

		scheduler.closedCallback(event)
		scheduler.finishedExecutions(event)
		switch scheduler.Status {
		case PendingStatus:
			scheduler.processEventOnPending(event)
		case ActiveStatus:
			scheduler.processEventOnActive(event)
		case InactiveStatus:
			scheduler.processEventOnInactive(event)
		case ClosingStatus:
			scheduler.processEventOnClosing(event)
		case ClosedStatus:
			scheduler.processEventOnClosed(event)
		case ErrorStatus:
			scheduler.processEventOnError(event)
		case CrashedStatus:
			scheduler.processEventOnCrashed(event)
		case ShutdownStatus:
			scheduler.processEventOnShutdown(event)
		}

		scheduler.lock.Unlock()
	}
}

func (scheduler *Scheduler) closedCallback(event ExecutionEvent) {
	if (event & callbackEventsMask) == callbackEventsMask {
		scheduler.callbackRunning = false
	}
}

func (scheduler *Scheduler) finishedExecutions(event ExecutionEvent) {
	switch event & finishMasks {
	case parallelFinishMask:
		scheduler.parallelRunning -= 1
	case serialCalledFinishMask:
		scheduler.currentSerial = nil
		scheduler.serialRunning -= 1
	case serialExpiredFinishMask:
		scheduler.serialRunning -= 1
	}
}

func (scheduler *Scheduler) processEventOnPending(event ExecutionEvent) {
	switch event {
	case PreparedEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.setStatus(ActiveStatus)
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case CrashedEvent:
		scheduler.setStatus(CrashedStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler) processEventOnActive(event ExecutionEvent) {
	switch event {
	case ScheduledEvent:
		scheduler.execute()
	case FinishedParallelEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.execute()
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case ErrorParallelEvent:
		scheduler.setStatus(ErrorStatus)
	case FinishedSerialEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.execute()
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case ExpiredFinishedSerialEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.execute()
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case ErrorSerialEvent:
		scheduler.setStatus(ErrorStatus)
	case ExpiredSerialEvent:
		scheduler.setStatus(ErrorStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler) processEventOnInactive(event ExecutionEvent) {
	switch event {
	case ScheduledEvent:
		scheduler.setStatus(ActiveStatus)
	case WakedEvent:
		scheduler.setStatus(ClosingStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler) processEventOnClosing(event ExecutionEvent) {
	switch event {
	case ClosingEvent:
		if scheduler.isRunning() || scheduler.isScheduled() {
			scheduler.setStatus(ActiveStatus)
		} else {
			scheduler.setStatus(ClosedStatus)
		}
	case CrashedEvent:
		scheduler.setStatus(CrashedStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler) processEventOnClosed(event ExecutionEvent) {
	if event == ScheduledEvent {
		scheduler.cancelExecutions()
	}
}

func (scheduler *Scheduler) processEventOnError(event ExecutionEvent) {
	switch event {
	case FinishedParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case FinishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case ExpiredFinishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case RefreshEvent:
		scheduler.setStatus(PendingStatus)
	case OnErrorFinishedEvent:
		if !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case ErrorParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case ErrorSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case ExpiredSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}

	case CrashedEvent:
		scheduler.setStatus(CrashedStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler) processEventOnCrashed(event ExecutionEvent) {
	switch event {
	case ScheduledEvent:
		scheduler.cancelExecutions()
	case FinishedParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case FinishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case ExpiredFinishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case OnCrashFinishedEvent:
		if !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case ErrorParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case ErrorSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case ExpiredSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler) processEventOnShutdown(event ExecutionEvent) {
	switch event {
	case CrashedEvent:
		scheduler.setStatus(CrashedStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	default:
		scheduler.tryToClose()
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
	case ShutdownStatus:
		scheduler.runOnShutdown()
	}
}

func (scheduler *Scheduler) execute() {
	for execution := scheduler.parallelQueue.Pop(); execution != nil; execution = scheduler.parallelQueue.Pop() {
		go execution.call(scheduler)
	}

	if scheduler.currentSerial == nil {
		execution := scheduler.serialQueue.Pop()
		if execution != nil {
			go execution.call(scheduler)
		}
	}
}

func (scheduler *Scheduler) cancelExecutions() {
	err := NewSchedulerCrashedError()

	for execution := scheduler.parallelQueue.Pop(); execution != nil; execution = scheduler.parallelQueue.Pop() {
		go execution.expire(scheduler, err)
	}

	for execution := scheduler.serialQueue.Pop(); execution != nil; execution = scheduler.serialQueue.Pop() {
		go execution.expire(scheduler, err)
	}
}

func (scheduler *Scheduler) beforeExecutionCall(execution *Execution) {
	if execution.kind == Parallel {
		scheduler.parallelRunning += 1
		return
	}

	scheduler.currentSerial = execution
	scheduler.serialRunning += 1
}

func (scheduler *Scheduler) beforeExpireCall(execution *Execution) {
	if execution.kind == Parallel {
		scheduler.parallelRunning += 1
		return
	}

	scheduler.serialRunning += 1
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

func (scheduler *Scheduler) isRunning() bool {
	return scheduler.parallelRunning > 0 || scheduler.serialRunning > 0
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

func (scheduler *Scheduler) runOnCrash() {
	scheduler.callbackRunning = true
	scheduler.cancelExecutions()

	if scheduler.options.onCrash == nil {
		scheduler.signal(OnCrashFinishedEvent)
		return
	}

	go func() {
		scheduler.options.onCrash(scheduler)
		scheduler.signal(OnCrashFinishedEvent)
	}()
}

func (scheduler *Scheduler) runOnShutdown() {
	scheduler.Err = NewShutdownError()
	scheduler.tryToClose()
}

func (scheduler *Scheduler) tryToClose() {
	if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
		scheduler.setStatus(ClosedStatus)
	} else {
		scheduler.cancelExecutions()
	}
}

func (scheduler *Scheduler) runOnLeaveErrorCallback() {
	scheduler.callbackRunning = true

	if scheduler.options.onLeaveError == nil {
		if scheduler.Err == nil {
			scheduler.Err = NewSchedulerNotRecovered()
		}
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
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(scheduler.options.onPrepare, PreparedEvent)
}

func (scheduler *Scheduler) runOnClosingCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(scheduler.options.onClosing, ClosingEvent)
}

func (scheduler *Scheduler) runCallbackAndFireEvent(callback func(*Scheduler) error, event ExecutionEvent) {
	if callback == nil {
		scheduler.signal(event)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(callback, event)
}

func (scheduler *Scheduler) runAsyncCallbackAndFireEvent(callback func(*Scheduler) error, event ExecutionEvent) {
	err := callback(scheduler)
	if err == nil {
		scheduler.signal(event)
		return
	}

	scheduler.Err = err
	scheduler.signal(CrashedEvent)
}
