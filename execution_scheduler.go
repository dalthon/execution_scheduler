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

type schedulerEvent uint64

const callbackEventsMask = 0x100

const parallelFinishMask = 0x200
const serialCalledFinishMask = 0x400
const serialExpiredFinishMask = 0x800
const finishMasks = parallelFinishMask | serialCalledFinishMask | serialExpiredFinishMask

const (
	scheduledEvent schedulerEvent = iota
	shutdownEvent
	wakedEvent

	preparedEvent schedulerEvent = callbackEventsMask | iota
	crashedEvent
	refreshEvent
	closingEvent
	onErrorFinishedEvent
	onCrashFinishedEvent

	finishedParallelEvent schedulerEvent = parallelFinishMask | iota
	errorParallelEvent

	finishedSerialEvent schedulerEvent = serialCalledFinishMask | iota
	errorSerialEvent

	expiredFinishedSerialEvent schedulerEvent = serialExpiredFinishMask | iota
	expiredSerialEvent
)

type schedulerInterface[C any] interface {
	beforeExecutionCall(execution *Execution[C])
	beforeExpireCall(execution *Execution[C])
	getLock() *sync.Mutex
	getClock() clockwork.Clock
	getContext() C
	signal(event schedulerEvent)
	remove(execution *Execution[C])
	Schedule(handler func(C) error, errorHandler func(C, error) error, kind ExecutionKind, priority int) *Execution[C]
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
	currentSerial   *Execution[C]
	parallelQueue   *executionQueue[C]
	serialQueue     *executionQueue[C]
	events          chan schedulerEvent
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
		parallelQueue:   newExecutionQueue[C](),
		serialQueue:     newExecutionQueue[C](),
		events:          make(chan schedulerEvent, 16),
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

func (scheduler *Scheduler[C]) Schedule(handler func(C) error, errorHandler func(C, error) error, kind ExecutionKind, priority int) *Execution[C] {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	var execution *Execution[C]
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
	scheduler.signal(scheduledEvent)

	return execution
}

func (scheduler *Scheduler[C]) Run() {
	scheduler.startedAt = scheduler.clock.Now()
	scheduler.runPrepareCallback()
}

func (scheduler *Scheduler[C]) Shutdown() {
	scheduler.signal(shutdownEvent)
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

func (scheduler *Scheduler[C]) closedCallback(event schedulerEvent) {
	if (event & callbackEventsMask) == callbackEventsMask {
		scheduler.callbackRunning = false
	}
}

func (scheduler *Scheduler[C]) finishedExecutions(event schedulerEvent) {
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

func (scheduler *Scheduler[C]) processEventOnPending(event schedulerEvent) {
	switch event {
	case preparedEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.setStatus(ActiveStatus)
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case crashedEvent:
		scheduler.setStatus(CrashedStatus)
	case shutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler[C]) processEventOnActive(event schedulerEvent) {
	switch event {
	case scheduledEvent:
		scheduler.execute()
	case finishedParallelEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.execute()
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case errorParallelEvent:
		scheduler.setStatus(ErrorStatus)
	case finishedSerialEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.execute()
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case expiredFinishedSerialEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.execute()
		} else {
			scheduler.setStatus(InactiveStatus)
		}
	case errorSerialEvent:
		scheduler.setStatus(ErrorStatus)
	case expiredSerialEvent:
		scheduler.setStatus(ErrorStatus)
	case shutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler[C]) processEventOnInactive(event schedulerEvent) {
	switch event {
	case scheduledEvent:
		scheduler.leaveInactive()
	case wakedEvent:
		if scheduler.isScheduled() || scheduler.isRunning() {
			scheduler.setStatus(ActiveStatus)
		} else {
			scheduler.setStatus(ClosingStatus)
		}
	case crashedEvent:
		scheduler.setStatus(CrashedStatus)
	case shutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler[C]) processEventOnClosing(event schedulerEvent) {
	switch event {
	case closingEvent:
		if scheduler.isRunning() || scheduler.isScheduled() {
			scheduler.setStatus(ActiveStatus)
		} else {
			scheduler.setStatus(ClosedStatus)
		}
	case crashedEvent:
		scheduler.setStatus(CrashedStatus)
	case shutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler[C]) processEventOnClosed(event schedulerEvent) {
	if event == scheduledEvent {
		scheduler.cancelExecutions()
	}
}

func (scheduler *Scheduler[C]) processEventOnError(event schedulerEvent) {
	switch event {
	case finishedParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case finishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case expiredFinishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case refreshEvent:
		scheduler.setStatus(PendingStatus)
	case onErrorFinishedEvent:
		if !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case errorParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case errorSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}
	case expiredSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() {
			scheduler.runOnLeaveErrorCallback()
		}

	case crashedEvent:
		scheduler.setStatus(CrashedStatus)
	case shutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler[C]) processEventOnCrashed(event schedulerEvent) {
	switch event {
	case scheduledEvent:
		scheduler.cancelExecutions()
	case finishedParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case finishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case expiredFinishedSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case onCrashFinishedEvent:
		if !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case errorParallelEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case errorSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case expiredSerialEvent:
		if !scheduler.callbackRunning && !scheduler.isRunning() && !scheduler.isScheduled() {
			scheduler.setStatus(ClosedStatus)
		}
	case shutdownEvent:
		scheduler.setStatus(ShutdownStatus)
	}
}

func (scheduler *Scheduler[C]) processEventOnShutdown(event schedulerEvent) {
	switch event {
	case crashedEvent:
		scheduler.setStatus(CrashedStatus)
	case shutdownEvent:
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

func (scheduler *Scheduler[C]) beforeExecutionCall(execution *Execution[C]) {
	if execution.kind == Parallel {
		scheduler.parallelRunning += 1
		return
	}

	scheduler.currentSerial = execution
	scheduler.serialRunning += 1
}

func (scheduler *Scheduler[C]) beforeExpireCall(execution *Execution[C]) {
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

func (scheduler *Scheduler[C]) getContext() C {
	return scheduler.Context
}

func (scheduler *Scheduler[C]) signal(event schedulerEvent) {
	scheduler.events <- event
}

func (scheduler *Scheduler[C]) remove(execution *Execution[C]) {
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
		scheduler.signal(wakedEvent)
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
			scheduler.signal(wakedEvent)
			scheduler.lock.Unlock()
			return
		}

		if err != nil && scheduler.Err == nil {
			scheduler.Err = err
		}
		scheduler.lock.Unlock()

		if err != nil {
			scheduler.signal(crashedEvent)
			return
		}
	}

	delay := scheduler.options.InactivityDelay - scheduler.clock.Since(inactivityStart)
	if delay <= time.Duration(0) {
		scheduler.signal(wakedEvent)
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
		scheduler.signal(wakedEvent)
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
			scheduler.signal(wakedEvent)
		} else {
			if scheduler.Err == nil {
				scheduler.Err = err
			}
			scheduler.signal(crashedEvent)
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
		scheduler.signal(onCrashFinishedEvent)
		return
	}

	go func() {
		scheduler.options.OnCrash(scheduler)
		scheduler.signal(onCrashFinishedEvent)
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
		scheduler.signal(crashedEvent)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(panicProofCallback(scheduler.options.OnLeaveError, "OnLeaveError"), refreshEvent)
}

func (scheduler *Scheduler[C]) runOnErrorCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.OnError, "OnError"), onErrorFinishedEvent)
}

func (scheduler *Scheduler[C]) runPrepareCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.OnPrepare, "OnPrepare"), preparedEvent)
}

func (scheduler *Scheduler[C]) runOnClosingCallback() {
	scheduler.callbackRunning = true
	scheduler.runCallbackAndFireEvent(panicProofCallback(scheduler.options.OnClosing, "OnClosing"), closingEvent)
}

func (scheduler *Scheduler[C]) runCallbackAndFireEvent(callback func(*Scheduler[C]) error, event schedulerEvent) {
	if callback == nil {
		scheduler.signal(event)
		return
	}

	go scheduler.runAsyncCallbackAndFireEvent(callback, event)
}

func (scheduler *Scheduler[C]) runAsyncCallbackAndFireEvent(callback func(*Scheduler[C]) error, event schedulerEvent) {
	err := callback(scheduler)
	if err == nil {
		scheduler.signal(event)
		return
	}

	scheduler.Err = err
	scheduler.signal(crashedEvent)
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
