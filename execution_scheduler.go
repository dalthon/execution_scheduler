package execution_scheduler

import (
	"math"
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

const parallelFinishMask = 0x200
const serialCalledFinishMask = 0x400
const serialExpiredFinishMask = 0x800
const finishMasks = parallelFinishMask | serialCalledFinishMask | serialExpiredFinishMask

const (
	ScheduledEvent ExecutionEvent = iota
	ShutdownEvent
	WakedEvent

	PreparedEvent ExecutionEvent = callbackEventsMask | iota
	CrashedEvent
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
	onError          func(scheduler *Scheduler) error
	onLeaveError     func(scheduler *Scheduler) error
	onInactive       func(scheduler *Scheduler) error
	onLeaveInactive  func(scheduler *Scheduler) error
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
	startedAt       time.Time
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
	scheduler.startedAt = scheduler.clock.Now()
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
		scheduler.leaveInactive()
	case WakedEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.setStatus(ActiveStatus)
		} else {
			scheduler.setStatus(ClosingStatus)
		}
	case CrashedEvent:
		scheduler.setStatus(CrashedStatus)
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

	if scheduler.Status == InactiveStatus {
		scheduler.runOnLeaveInactive()
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
	if scheduler.currentSerial != nil && scheduler.currentSerial.kind == Critical {
		return
	}

	priority := math.MinInt
	topSerialExecution := scheduler.serialQueue.Top()
	if topSerialExecution != nil && topSerialExecution.kind == Critical {
		priority = topSerialExecution.priority
	}

	for execution := scheduler.parallelQueue.PopPriority(priority); execution != nil; execution = scheduler.parallelQueue.PopPriority(priority) {
		execution.call(scheduler)
	}

	if scheduler.currentSerial == nil && topSerialExecution != nil && (topSerialExecution.kind != Critical || scheduler.parallelRunning == 0) {
		execution := scheduler.serialQueue.Pop()
		if execution != nil {
			execution.call(scheduler)
		}
	}
}

func (scheduler *Scheduler) cancelExecutions() {
	err := NewSchedulerCrashedError()

	for execution := scheduler.parallelQueue.Pop(); execution != nil; execution = scheduler.parallelQueue.Pop() {
		execution.expire(scheduler, err)
	}

	for execution := scheduler.serialQueue.Pop(); execution != nil; execution = scheduler.serialQueue.Pop() {
		execution.expire(scheduler, err)
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

	go scheduler.inactiveWaiting()
}

func (scheduler *Scheduler) inactiveWaiting() {
	inactivityStart := scheduler.clock.Now()
	if scheduler.options.onInactive != nil {

		scheduler.lock.Lock()
		scheduler.callbackRunning = true
		scheduler.lock.Unlock()

		err := (panicProofCallback(scheduler.options.onInactive, "OnInactive"))(scheduler)

		scheduler.lock.Lock()
		scheduler.callbackRunning = false
		if err == nil && (scheduler.isScheduled() || scheduler.isRunning()) {
			scheduler.signal(WakedEvent)
			scheduler.lock.Unlock()
			return
		}

		if err != nil && scheduler.Err == nil {
			scheduler.Err = err
		}
		scheduler.lock.Unlock()

		if err != nil {
			scheduler.signal(CrashedEvent)
			return
		}
	}

	delay := scheduler.options.inactivityDelay - scheduler.clock.Since(inactivityStart)
	if delay <= time.Duration(0) {
		scheduler.signal(WakedEvent)
		return
	}

	scheduler.inactivityTimer = scheduler.clock.AfterFunc(
		delay,
		scheduler.wakeFromInactivity,
	)
}

func (scheduler *Scheduler) runOnLeaveInactive() {
	if scheduler.inactivityTimer != nil {
		scheduler.inactivityTimer.Stop()
		scheduler.inactivityTimer = nil
	}
}

func (scheduler *Scheduler) wakeFromInactivity() {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if scheduler.inactivityTimer != nil {
		scheduler.inactivityTimer.Stop()
		scheduler.inactivityTimer = nil
	}

	if scheduler.options.onLeaveInactive == nil {
		scheduler.signal(WakedEvent)
		return
	}

	scheduler.leaveInactive()
}

func (scheduler *Scheduler) leaveInactive() {
	if scheduler.callbackRunning {
		return
	}

	if scheduler.options.onLeaveInactive == nil {
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.setStatus(ActiveStatus)
		}
		return
	}

	scheduler.callbackRunning = true

	go func() {
		err := (panicProofCallback(scheduler.options.onLeaveInactive, "OnLeaveInactive"))(scheduler)

		scheduler.lock.Lock()
		defer scheduler.lock.Unlock()

		scheduler.callbackRunning = false
		if err == nil {
			scheduler.signal(WakedEvent)
		} else {
			if scheduler.Err == nil {
				scheduler.Err = err
			}
			scheduler.signal(CrashedEvent)
		}
	}()
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

	go scheduler.runAsyncCallbackAndFireEvent(panicProofCallback(scheduler.options.onLeaveError, "OnLeaveError"), RefreshEvent)
}

func (scheduler *Scheduler) runOnErrorCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.onError, "OnError"), OnErrorFinishedEvent)
}

func (scheduler *Scheduler) runPrepareCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.onPrepare, "OnPrepare"), PreparedEvent)
}

func (scheduler *Scheduler) runOnClosingCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.onClosing, "OnClosing"), ClosingEvent)
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

func panicProofCallback(callback func(*Scheduler) error, target string) func(*Scheduler) error {
	if callback == nil {
		return nil
	}

	return func(scheduler *Scheduler) error {
		errorChannel := make(chan error)

		go func() {
			defer func() {
				if recovery := recover(); recovery != nil {
					errorChannel <- NewPanicError(target, recovery)
				}
			}()

			err := callback(scheduler)
			errorChannel <- err
		}()

		return <-errorChannel
	}
}
