// This Execution Scheduler package can be used to handle complex concurrency
// executions in an easy fashion.
//
// The main entity is [Scheduler], which could be instantiated by [NewScheduler].
//
// Once we have a scheduler, we need to make it run with [Scheduler.Run], we can
// asyncronously schedule executions with [Scheduler.Schedule] and force it to shutdown
// with [Scheduler.Shutdown].
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

type Status uint64

const (
	PendingStatus Status = iota
	ActiveStatus
	InactiveStatus
	ClosingStatus
	ClosedStatus
	ErrorStatus
	CrashedStatus
	ShutdownStatus
)

type Event uint64

const callbackEventsMask = 0x100

const parallelFinishMask = 0x200
const serialCalledFinishMask = 0x400
const serialExpiredFinishMask = 0x800
const finishMasks = parallelFinishMask | serialCalledFinishMask | serialExpiredFinishMask

const (
	ScheduledEvent Event = iota
	ShutdownEvent
	WakedEvent

	PreparedEvent Event = callbackEventsMask | iota
	CrashedEvent
	RefreshEvent
	ClosingEvent
	OnErrorFinishedEvent
	OnCrashFinishedEvent

	FinishedParallelEvent Event = parallelFinishMask | iota
	ErrorParallelEvent

	FinishedSerialEvent Event = serialCalledFinishMask | iota
	ErrorSerialEvent

	ExpiredFinishedSerialEvent Event = serialExpiredFinishMask | iota
	ExpiredSerialEvent
)

type schedulerInterface interface {
	beforeExecutionCall(execution *Execution)
	beforeExpireCall(execution *Execution)
	getLock() *sync.Mutex
	getClock() clockwork.Clock
	signal(event Event)
	remove(execution *Execution)
	Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution
}

type Options[C any] struct {
	ExecutionTimeout time.Duration
	InactivityDelay  time.Duration
	OnPrepare        func(scheduler *Scheduler[C]) error
	OnClosing        func(scheduler *Scheduler[C]) error
	OnError          func(scheduler *Scheduler[C]) error
	OnLeaveError     func(scheduler *Scheduler[C]) error
	OnInactive       func(scheduler *Scheduler[C]) error
	OnLeaveInactive  func(scheduler *Scheduler[C]) error
	OnCrash          func(scheduler *Scheduler[C])
	OnClose          func(scheduler *Scheduler[C])
}

type Scheduler[C any] struct {
	Status          Status
	Err             error
	Context         C
	options         *Options[C]
	lock            sync.Mutex
	parallelRunning uint32
	serialRunning   uint32
	currentSerial   *Execution
	parallelQueue   *executionQueue
	serialQueue     *executionQueue
	events          chan Event
	callbackRunning bool
	clock           clockwork.Clock
	waitGroup       *sync.WaitGroup
	inactivityTimer clockwork.Timer
	startedAt       time.Time
}

func NewScheduler[C any](options *Options[C], context C, waitGroup *sync.WaitGroup) *Scheduler[C] {
	scheduler := &Scheduler[C]{
		Status:          PendingStatus,
		Context:         context,
		options:         options,
		parallelRunning: 0,
		serialRunning:   0,
		currentSerial:   nil,
		parallelQueue:   newExecutionQueue(),
		serialQueue:     newExecutionQueue(),
		events:          make(chan Event, 16),
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

func (scheduler *Scheduler[C]) Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
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

	if scheduler.options.ExecutionTimeout != time.Duration(0) {
		execution.setExpiration(scheduler, scheduler.options.ExecutionTimeout)
	}
	scheduler.signal(ScheduledEvent)

	return execution
}

func (scheduler *Scheduler[C]) Run() {
	scheduler.startedAt = scheduler.clock.Now()
	scheduler.runPrepareCallback()
}

func (scheduler *Scheduler[C]) Shutdown() {
	scheduler.signal(ShutdownEvent)
}

func (scheduler *Scheduler[C]) eventLoop() {
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

func (scheduler *Scheduler[C]) closedCallback(event Event) {
	if (event & callbackEventsMask) == callbackEventsMask {
		scheduler.callbackRunning = false
	}
}

func (scheduler *Scheduler[C]) finishedExecutions(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnPending(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnActive(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnInactive(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnClosing(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnClosed(event Event) {
	if event == ScheduledEvent {
		scheduler.cancelExecutions()
	}
}

func (scheduler *Scheduler[C]) processEventOnError(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnCrashed(event Event) {
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

func (scheduler *Scheduler[C]) processEventOnShutdown(event Event) {
	switch event {
	case CrashedEvent:
		scheduler.setStatus(CrashedStatus)
	case ShutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	default:
		scheduler.tryToClose()
	}
}

func (scheduler *Scheduler[C]) setStatus(status Status) {
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

func (scheduler *Scheduler[C]) execute() {
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

func (scheduler *Scheduler[C]) cancelExecutions() {
	err := newSchedulerCrashedError()

	for execution := scheduler.parallelQueue.Pop(); execution != nil; execution = scheduler.parallelQueue.Pop() {
		execution.expire(scheduler, err)
	}

	for execution := scheduler.serialQueue.Pop(); execution != nil; execution = scheduler.serialQueue.Pop() {
		execution.expire(scheduler, err)
	}
}

func (scheduler *Scheduler[C]) beforeExecutionCall(execution *Execution) {
	if execution.kind == Parallel {
		scheduler.parallelRunning += 1
		return
	}

	scheduler.currentSerial = execution
	scheduler.serialRunning += 1
}

func (scheduler *Scheduler[C]) beforeExpireCall(execution *Execution) {
	if execution.kind == Parallel {
		scheduler.parallelRunning += 1
		return
	}

	scheduler.serialRunning += 1
}

func (scheduler *Scheduler[C]) getLock() *sync.Mutex {
	return &scheduler.lock
}

func (scheduler *Scheduler[C]) getClock() clockwork.Clock {
	return scheduler.clock
}

func (scheduler *Scheduler[C]) signal(event Event) {
	scheduler.events <- event
}

func (scheduler *Scheduler[C]) remove(execution *Execution) {
	if execution.kind == Parallel {
		scheduler.parallelQueue.Remove(execution)
	} else {
		scheduler.serialQueue.Remove(execution)
	}
}

func (scheduler *Scheduler[C]) isRunning() bool {
	return scheduler.parallelRunning > 0 || scheduler.serialRunning > 0
}

func (scheduler *Scheduler[C]) isScheduled() bool {
	return scheduler.serialQueue.Size() > 0 || scheduler.parallelQueue.Size() > 0
}

func (scheduler *Scheduler[C]) runOnInactive() {
	if scheduler.options.InactivityDelay == time.Duration(0) {
		scheduler.signal(WakedEvent)
		return
	}

	go scheduler.inactiveWaiting()
}

func (scheduler *Scheduler[C]) inactiveWaiting() {
	inactivityStart := scheduler.clock.Now()
	if scheduler.options.OnInactive != nil {

		scheduler.lock.Lock()
		scheduler.callbackRunning = true
		scheduler.lock.Unlock()

		err := (panicProofCallback(scheduler.options.OnInactive, "OnInactive"))(scheduler)

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

	delay := scheduler.options.InactivityDelay - scheduler.clock.Since(inactivityStart)
	if delay <= time.Duration(0) {
		scheduler.signal(WakedEvent)
		return
	}

	scheduler.inactivityTimer = scheduler.clock.AfterFunc(
		delay,
		scheduler.wakeFromInactivity,
	)
}

func (scheduler *Scheduler[C]) runOnLeaveInactive() {
	if scheduler.inactivityTimer != nil {
		scheduler.inactivityTimer.Stop()
		scheduler.inactivityTimer = nil
	}
}

func (scheduler *Scheduler[C]) wakeFromInactivity() {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if scheduler.inactivityTimer != nil {
		scheduler.inactivityTimer.Stop()
		scheduler.inactivityTimer = nil
	}

	if scheduler.options.OnLeaveInactive == nil {
		scheduler.signal(WakedEvent)
		return
	}

	scheduler.leaveInactive()
}

func (scheduler *Scheduler[C]) leaveInactive() {
	if scheduler.callbackRunning {
		return
	}

	if scheduler.options.OnLeaveInactive == nil {
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.setStatus(ActiveStatus)
		}
		return
	}

	scheduler.callbackRunning = true

	go func() {
		err := (panicProofCallback(scheduler.options.OnLeaveInactive, "OnLeaveInactive"))(scheduler)

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

func (scheduler *Scheduler[C]) runOnClose() {
	if scheduler.waitGroup != nil {
		scheduler.waitGroup.Done()
	}

	if scheduler.options.OnClose != nil {
		go scheduler.options.OnClose(scheduler)
	}
}

func (scheduler *Scheduler[C]) runOnPrepare() {
	scheduler.Err = nil
	scheduler.runPrepareCallback()
}

func (scheduler *Scheduler[C]) runOnCrash() {
	scheduler.callbackRunning = true
	scheduler.cancelExecutions()

	if scheduler.options.OnCrash == nil {
		scheduler.signal(OnCrashFinishedEvent)
		return
	}

	go func() {
		scheduler.options.OnCrash(scheduler)
		scheduler.signal(OnCrashFinishedEvent)
	}()
}

func (scheduler *Scheduler[C]) runOnShutdown() {
	scheduler.Err = newShutdownError()
	scheduler.tryToClose()
}

func (scheduler *Scheduler[C]) tryToClose() {
	if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
		scheduler.setStatus(ClosedStatus)
	} else {
		scheduler.cancelExecutions()
	}
}

func (scheduler *Scheduler[C]) runOnLeaveErrorCallback() {
	scheduler.callbackRunning = true

	if scheduler.options.OnLeaveError == nil {
		if scheduler.Err == nil {
			scheduler.Err = newSchedulerNotRecovered()
		}
		scheduler.signal(CrashedEvent)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(panicProofCallback(scheduler.options.OnLeaveError, "OnLeaveError"), RefreshEvent)
}

func (scheduler *Scheduler[C]) runOnErrorCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.OnError, "OnError"), OnErrorFinishedEvent)
}

func (scheduler *Scheduler[C]) runPrepareCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.OnPrepare, "OnPrepare"), PreparedEvent)
}

func (scheduler *Scheduler[C]) runOnClosingCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.OnClosing, "OnClosing"), ClosingEvent)
}

func (scheduler *Scheduler[C]) runCallbackAndFireEvent(callback func(*Scheduler[C]) error, event Event) {
	if callback == nil {
		scheduler.signal(event)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(callback, event)
}

func (scheduler *Scheduler[C]) runAsyncCallbackAndFireEvent(callback func(*Scheduler[C]) error, event Event) {
	err := callback(scheduler)
	if err == nil {
		scheduler.signal(event)
		return
	}

	scheduler.Err = err
	scheduler.signal(CrashedEvent)
}

func panicProofCallback[C any](callback func(*Scheduler[C]) error, target string) func(*Scheduler[C]) error {
	if callback == nil {
		return nil
	}

	return func(scheduler *Scheduler[C]) error {
		errorChannel := make(chan error)

		go func() {
			defer func() {
				if recovery := recover(); recovery != nil {
					errorChannel <- newPanicError(target, recovery)
				}
			}()

			err := callback(scheduler)
			errorChannel <- err
		}()

		return <-errorChannel
	}
}
