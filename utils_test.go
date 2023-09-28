package execution_scheduler

import (
	"reflect"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"testing"

	"github.com/jonboulle/clockwork"
)

func defaultSchedulerOptions() *SchedulerOptions {
	return &SchedulerOptions{
		executionTimeout: time.Duration(0),
		inactivityDelay:  time.Duration(0),
		onPrepare:        nil,
		onClosing:        nil,
		onLeaveError:     nil,
		onError:          nil,
		onCrash:          nil,
		onClose:          nil,
	}
}

func schedulerEventToString(event ExecutionEvent) string {
	switch event {
	case PreparedEvent:
		return "Prepared"
	case ScheduledEvent:
		return "Scheduled"
	case FinishedParallelEvent:
		return "FinishedParallel"
	case FinishedSerialEvent:
		return "FinishedSerial"
	case WakedEvent:
		return "Waked"
	case ClosingEvent:
		return "Closing"
	case ErrorParallelEvent:
		return "Error"
	case ErrorSerialEvent:
		return "ErrorSerial"
	case OnErrorFinishedEvent:
		return "OnErrorFinished"
	case OnCrashFinishedEvent:
		return "OnCrashFinished"
	case RefreshEvent:
		return "Refresh"
	case CrashedEvent:
		return "Crashed"
	case ShutdownEvent:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

func schedulerStatusToString(status SchedulerStatus) string {
	switch status {
	case PendingStatus:
		return "Pending"
	case ActiveStatus:
		return "Active"
	case InactiveStatus:
		return "Inactive"
	case ClosingStatus:
		return "Closing"
	case ClosedStatus:
		return "Closed"
	case ErrorStatus:
		return "Error"
	case CrashedStatus:
		return "Crashed"
	case ShutdownStatus:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

type mockedScheduler struct {
	lock                     sync.Mutex
	events                   []ExecutionEvent
	removedExecutions        []*Execution
	clock                    clockwork.Clock
	beforeExecutionCallCount uint64
	beforeExpireCallCount    uint64
}

func newMockedScheduler() *mockedScheduler {
	return &mockedScheduler{
		clock:                    clockwork.NewFakeClock(),
		beforeExecutionCallCount: 0,
		beforeExpireCallCount:    0,
	}
}

func (scheduler *mockedScheduler) Schedule(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
	return NewExecution(
		handler,
		errorHandler,
		kind,
		priority,
	)
}

func (scheduler *mockedScheduler) beforeExecutionCall(execution *Execution) {
	scheduler.beforeExecutionCallCount += 1
}

func (scheduler *mockedScheduler) beforeExpireCall(execution *Execution) {
	scheduler.beforeExpireCallCount += 1
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
	waitForAllGoroutines()
}

type simpleHandlers struct {
	t                   *testing.T
	maxCount            int
	handlerCount        int
	errorHandlerCount   int
	handlerChannel      chan bool
	errorHandlerChannel chan bool
}

func newSimpleHandlers(t *testing.T, maxCount int) *simpleHandlers {
	return &simpleHandlers{
		t:                   t,
		handlerCount:        0,
		errorHandlerCount:   0,
		maxCount:            maxCount,
		handlerChannel:      make(chan bool),
		errorHandlerChannel: make(chan bool),
	}
}

func (pair *simpleHandlers) handler() error {
	if pair.handlerCount > pair.maxCount {
		pair.t.Fatalf("simple handler should not be called more than %d", pair.maxCount)
	}
	pair.handlerCount += 1
	pair.handlerChannel <- true
	return nil
}

func (pair *simpleHandlers) errorHandler(err error) error {
	if pair.errorHandlerCount > pair.maxCount {
		pair.t.Fatalf("simple handler error should not be called more than %d", pair.maxCount)
	}
	pair.errorHandlerCount += 1
	pair.errorHandlerChannel <- true
	return err
}

func (pair *simpleHandlers) wait() {
	select {
	case <-pair.handlerChannel:
	case <-pair.errorHandlerChannel:
	}
	waitForAllGoroutines()
}

func executionStatusToString(status ExecutionStatus) string {
	switch status {
	case ExecutionScheduled:
		return "Scheduled"
	case ExecutionRunning:
		return "Running"
	case ExecutionExpired:
		return "Expired"
	case ExecutionFinished:
		return "Finished"
	default:
		return "Unknown"
	}
}

type testTimelinesExample struct {
	t         *testing.T
	scheduler *Scheduler
	params    []testTimelineParams
}

type testDelayedHandlerParams struct {
	delay  int
	result error
}

func testDelayedHandler(delay int, result error) testDelayedHandlerParams {
	return testDelayedHandlerParams{
		delay:  delay,
		result: result,
	}
}

func testDummyHandler() testDelayedHandlerParams {
	return testDelayedHandlerParams{
		delay:  1,
		result: nil,
	}
}

type testTimelineParams struct {
	delay        int
	handler      testDelayedHandlerParams
	errorHandler testDelayedHandlerParams
	kind         ExecutionKind
	priority     int
}

type testExecutionStatus uint64

const (
	_esP testExecutionStatus = iota
	_esS
	_esR
	_esX
	_esF
)

type testTimelineExpectations struct {
	at         int
	status     SchedulerStatus
	executions []testExecutionStatus
	error      error
}

func newTestTimelinesExample(t *testing.T, scheduler *Scheduler, params []testTimelineParams) *testTimelinesExample {
	scheduler.clock = clockwork.NewFakeClock()

	return &testTimelinesExample{
		t:         t,
		scheduler: scheduler,
		params:    params,
	}
}

func (timelines *testTimelinesExample) expects(expectations []testTimelineExpectations, expCalledAt map[int]time.Duration, expErroredAt map[int]time.Duration) {
	var lock sync.Mutex

	clock := timelines.scheduler.clock.(clockwork.FakeClock)
	startedAt := clock.Now()

	calledAt := make(map[int]time.Duration)
	erroredAt := make(map[int]time.Duration)
	executions := make([]*Execution, len(timelines.params))
	scheduleExecution := func(params testTimelineParams, index int) {
		executions[index] = timelines.scheduler.Schedule(
			func() error {
				lock.Lock()
				calledAt[index] = clock.Since(startedAt)
				lock.Unlock()
				clock.Sleep(time.Duration(params.handler.delay) * time.Second)
				return params.handler.result
			},
			func(err error) error {
				lock.Lock()
				erroredAt[index] = clock.Since(startedAt)
				lock.Unlock()
				clock.Sleep(time.Duration(params.errorHandler.delay) * time.Second)
				return params.errorHandler.result
			},
			params.kind,
			params.priority,
		)
	}

	for i, params := range timelines.params {
		fixedParams := params
		index := i
		if params.delay == 0 {
			scheduleExecution(fixedParams, index)
		} else {
			clock.AfterFunc(
				time.Duration(fixedParams.delay)*time.Second,
				func() { scheduleExecution(fixedParams, index) },
			)
		}
	}

	timelines.scheduler.Run()
	waitForAllGoroutines()

	checkExpectation := func(expectation testTimelineExpectations) {
		if timelines.scheduler.Status != expectation.status {
			timelines.t.Fatalf("At %s expected scheduler status %q, but got %q", clock.Since(startedAt), schedulerStatusToString(expectation.status), schedulerStatusToString(timelines.scheduler.Status))
		}

		for i, execution := range expectation.executions {
			switch execution {
			case _esP:
				if executions[i] != nil {
					timelines.t.Fatalf("At %s expected execution %d to be pending, but is %q", clock.Since(startedAt), i, executionStatusToString(executions[i].Status))
				}
			case _esS:
				if executions[i] == nil {
					timelines.t.Fatalf("At %s expected execution %d to be scheduled, but is pending", clock.Since(startedAt), i)
				} else if executions[i].Status != ExecutionScheduled {
					timelines.t.Fatalf("At %s expected execution %d to be scheduled, but is %q", clock.Since(startedAt), i, executionStatusToString(executions[i].Status))
				}
			case _esR:
				if executions[i] == nil {
					timelines.t.Fatalf("At %s expected execution %d to be running, but is pending", clock.Since(startedAt), i)
				} else if executions[i].Status != ExecutionRunning {
					timelines.t.Fatalf("At %s expected execution %d to be running, but is %q", clock.Since(startedAt), i, executionStatusToString(executions[i].Status))
				}
			case _esX:
				if executions[i] == nil {
					timelines.t.Fatalf("At %s expected execution %d to be expired, but is pending", clock.Since(startedAt), i)
				} else if executions[i].Status != ExecutionExpired {
					timelines.t.Fatalf("At %s expected execution %d to be expired, but is %q", clock.Since(startedAt), i, executionStatusToString(executions[i].Status))
				}
			case _esF:
				if executions[i] == nil {
					timelines.t.Fatalf("At %s expected execution %d to be finished, but is pending", clock.Since(startedAt), i)
				} else if executions[i].Status != ExecutionFinished {
					timelines.t.Fatalf("At %s expected execution %d to be finished, but is %q", clock.Since(startedAt), i, executionStatusToString(executions[i].Status))
				}
			}
		}

		if timelines.scheduler.Err != nil && expectation.error != nil {
			if timelines.scheduler.Err.Error() != expectation.error.Error() {
				timelines.t.Fatalf("At %s expected scheduler error should be \"%v\", but got \"%v\"", clock.Since(startedAt), expectation.error, timelines.scheduler.Err)
			}
		} else {
			if timelines.scheduler.Err != nil || expectation.error != nil {
				timelines.t.Fatalf("At %s expected scheduler error should be \"%v\", but got \"%v\"", clock.Since(startedAt), expectation.error, timelines.scheduler.Err)
			}
		}
	}

	for _, expectation := range expectations {
		if expectation.at == 0 {
			checkExpectation(expectation)
			continue
		}

		clock.Advance(time.Duration(expectation.at)*time.Second - clock.Since(startedAt))
		waitForAllGoroutines()
		checkExpectation(expectation)
	}

	if !reflect.DeepEqual(calledAt, expCalledAt) {
		timelines.t.Fatalf("Expected executions to be called at %v, but got %v", expCalledAt, calledAt)
	}

	if !reflect.DeepEqual(erroredAt, expErroredAt) {
		timelines.t.Fatalf("Expected executions to be errored at %v, but got %v", expErroredAt, erroredAt)
	}
}

// HIC SUNT DRACONES:
//
//   Be brave and wise!
//   From here on down below we are entering uncharted territory!
//   Walk carefully and be aware of danger!
//
//   Code below depends on internal Golang implementation and
//   can break once we upgrade its version.

// waitForAllGoroutines() waits for 1000 consecutive attempts to release
// execution to other goroutines, so it is very unlikely that something is
// still waiting but unfortunally it is possible.
//
// IT IS HORRIBLE, I KNOW!
func waitForAllGoroutines() {
	limit := 1000

	for {
		runtime.Gosched()
		for i := 0; i <= limit; i++ {
			if shouldWaitForGoroutines() {
				runtime.Gosched()
				break
			}
			if i == limit {
				return
			}
		}
	}
}

const (
	_Grunnable = 1
	_Grunning  = 2
)

//go:linkname readgstatus runtime.readgstatus
//go:nosplit
func readgstatus(gp unsafe.Pointer) uint32

//go:linkname forEachG runtime.forEachG
func forEachG(fn func(gp unsafe.Pointer))

func shouldWaitForGoroutines() bool {
	runnable := 0
	running := 0

	forEachG(func(gp unsafe.Pointer) {
		switch readgstatus(gp) &^ 0x1000 {
		case _Grunnable:
			runnable++
		case _Grunning:
			running++
		}
	})

	return runnable != 0 || running != 1
}
