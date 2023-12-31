package execution_scheduler

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"testing"
)

func TestSchedulerEmptyTimeline(t *testing.T) {
	scheduler := NewScheduler(defaultSchedulerOptions(), nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{at: 0, status: ClosedStatus, executions: []testExecutionStatus{}},
		},
		map[int]time.Duration{},
		map[int]time.Duration{},
	)
}

func TestSchedulerMinimalSerialTimeline(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Serial, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{at: 0, status: InactiveStatus, executions: []testExecutionStatus{_esP}},
			{at: 1, status: ActiveStatus, executions: []testExecutionStatus{_esR}},
			{at: 2, status: InactiveStatus, executions: []testExecutionStatus{_esF}},
			{at: 3, status: InactiveStatus, executions: []testExecutionStatus{_esF}},
			{at: 4, status: ClosedStatus, executions: []testExecutionStatus{_esF}},
		},
		map[int]time.Duration{0: 1 * time.Second},
		map[int]time.Duration{},
	)
}

func TestSchedulerMinimalParallelTimeline(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{at: 0, status: InactiveStatus, executions: []testExecutionStatus{_esP}},
			{at: 1, status: ActiveStatus, executions: []testExecutionStatus{_esR}},
			{at: 2, status: InactiveStatus, executions: []testExecutionStatus{_esF}},
			{at: 3, status: InactiveStatus, executions: []testExecutionStatus{_esF}},
			{at: 4, status: ClosedStatus, executions: []testExecutionStatus{_esF}},
		},
		map[int]time.Duration{0: 1 * time.Second},
		map[int]time.Duration{},
	)
}

func TestSchedulerTimeout(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 8, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 16, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 20, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esP, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esP, _esP, _esP, _esP},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esR, _esP, _esP, _esP},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esR, _esP, _esP, _esP},
			},
			{
				at:         7,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esP, _esP, _esP},
			},
			{
				at:         8,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esS, _esP, _esP},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esS, _esP, _esP},
			},
			{
				at:         10,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         11,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         12,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         13,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         14,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         15,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         16,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esR, _esP},
			},
			{
				at:         17,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esP},
			},
			{
				at:         18,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esP},
			},
			{
				at:         19,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esP},
			},
			{
				at:         20,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esS},
			},
			{
				at:         21,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esS},
			},
			{
				at:         22,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esX},
			},
			{
				at:         23,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esX},
			},
		},
		map[int]time.Duration{
			1: 5 * time.Second,
			3: 16 * time.Second,
		},
		map[int]time.Duration{
			0: 3 * time.Second,
			1: 6 * time.Second,
			2: 10 * time.Second,
			4: 22 * time.Second,
		},
	)

	expectedPreparedAt := []time.Duration{4 * time.Second, 15 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	expectedLeftErrorAt := []time.Duration{11 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	expectedClosingAt := []time.Duration{23 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerAllPendingTransitions(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 6, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 13, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		if len(preparedAt) == 4 {
			return errors.New("Its enough!")
		}

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP},
			},
			{
				at:         5,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP},
			},
			{
				at:         6,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         10,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         11,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         12,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         13,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR},
			},
			{
				at:         14,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR},
			},
			{
				at:         15,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         16,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         17,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         18,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
				error:      errors.New("Its enough!"),
			},
			{
				at:         19,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
				error:      errors.New("Its enough!"),
			},
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 7 * time.Second,
			2: 13 * time.Second,
		},
		map[int]time.Duration{
			0: 3 * time.Second,
			1: 8 * time.Second,
			2: 14 * time.Second,
		},
	)

	expectedPreparedAt := []time.Duration{2 * time.Second, 7 * time.Second, 12 * time.Second, 18 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	expectedLeftErrorAt := []time.Duration{5 * time.Second, 10 * time.Second, 16 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeftError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Its enough!" {
		t.Fatalf("Scheduler should have finished with error message \"Its enough!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerSerialExpiration(t *testing.T) {
	options := defaultSchedulerOptions()
	options.ExecutionTimeout = 3 * time.Second
	errorHandler := testErrorHandlerBuilder()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: testDelayedHandler(1, errors.New("Boom!"))},
			{delay: 4, kind: Serial, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 9, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: testDelayedHandler(0, nil)},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(6 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return nil
	}

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	err := newSchedulerNotRecovered()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         2,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         3,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         4,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esS, _esP},
			},
			{
				at:         5,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esX, _esS, _esP},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esR, _esP},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esR, _esP},
			},
			{
				at:         8,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esP},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esS},
			},
			{
				at:         10,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esS},
			},
			{
				at:         11,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
				error:      err,
			},
		},
		map[int]time.Duration{
			1: 6 * time.Second,
		},
		map[int]time.Duration{
			0: 4 * time.Second,
			1: 7 * time.Second,
			2: 11 * time.Second,
		},
	)

	expectedPreparedAt := []time.Duration{6 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	expectedErrorAt := []time.Duration{11 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err.Error() != err.Error() {
		t.Fatalf("Scheduler should have finished with error \"%v\", but got \"%v\"", err, scheduler.Err)
	}
}

func TestSchedulerAllErrorTransitions(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 6, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		if len(leftErrorAt) == 2 {
			return errors.New("Its enough!")
		}

		return nil
	}

	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         5,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         6,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         10,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Its enough!"),
			},
			{
				at:         11,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Its enough!"),
			},
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 7 * time.Second,
		},
		map[int]time.Duration{
			0: 3 * time.Second,
			1: 8 * time.Second,
		},
	)

	expectedPreparedAt := []time.Duration{2 * time.Second, 7 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	expectedLeftErrorAt := []time.Duration{5 * time.Second, 10 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeftError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Its enough!" {
		t.Fatalf("Scheduler should have finished with error message \"Its enough!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerOnErrorOnCallabckIgnoresLeaveError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Boom #%d!", len(errorAt)))
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return errors.New("Should not be here!")
	}

	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom #1!"),
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom #1!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedLeftErrorAt := []time.Duration{}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeftError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom #1!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom #1!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerLeavesErrorWhenNotRunningExecutions(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esR},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR},
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
				error:      newSchedulerNotRecovered(),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
			2: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
		},
	)
}

func TestSchedulerLeavesErrorWhenNotRunningExecutionsWithError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: errorHandler(2), errorHandler: errorHandler(1)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      newSchedulerNotRecovered(),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 4 * time.Second,
		},
	)
}

func TestSchedulerLeavesErrorWhenNotRunningExecutionsWithCallbacks(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 4, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 5, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 10, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: errorHandler(1)},
			{delay: 13, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	erroredAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		erroredAt = append(erroredAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esP, _esP, _esP},
			},
			{
				at:         5,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esP, _esP},
			},
			{
				at:         6,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esP, _esP},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esP, _esP},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esP, _esP},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esP, _esP},
			},
			{
				at:         10,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS, _esP},
			},
			{
				at:         11,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS, _esP},
			},
			{
				at:         12,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esS, _esP},
			},
			{
				at:         13,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esS, _esS},
			},
			{
				at:         14,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR, _esR},
			},
			{
				at:         15,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR, _esR},
			},
			{
				at:         16,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         17,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         18,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
			2: 7 * time.Second,
			3: 7 * time.Second,
			4: 14 * time.Second,
			5: 14 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			2: 8 * time.Second,
		},
	)

	expectedErroredAt := []time.Duration{5 * time.Second, 11 * time.Second}
	if !reflect.DeepEqual(erroredAt, expectedErroredAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErroredAt, erroredAt)
	}

	expectedLeftErrorAt := []time.Duration{7 * time.Second, 14 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}
}

func TestSchedulerLeavesErrorWhenNotRunningExecutionsWithErrorAndCallbacks(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 5, kind: Parallel, priority: 0, handler: errorHandler(4), errorHandler: errorHandler(1)},
			{delay: 10, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: errorHandler(1)},
			{delay: 13, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	erroredAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		erroredAt = append(erroredAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esP, _esP, _esP},
			},
			{
				at:         5,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esP, _esP},
			},
			{
				at:         6,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esP, _esP},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esP, _esP},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esP, _esP},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esP, _esP},
			},
			{
				at:         10,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS, _esP},
			},
			{
				at:         11,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS, _esP},
			},
			{
				at:         12,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esS, _esP},
			},
			{
				at:         13,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esS, _esS},
			},
			{
				at:         14,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR, _esR},
			},
			{
				at:         15,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR, _esR},
			},
			{
				at:         16,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         17,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         18,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
			2: 7 * time.Second,
			3: 7 * time.Second,
			4: 14 * time.Second,
			5: 14 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
			2: 8 * time.Second,
			3: 11 * time.Second,
		},
	)

	expectedErroredAt := []time.Duration{5 * time.Second, 11 * time.Second}
	if !reflect.DeepEqual(erroredAt, expectedErroredAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErroredAt, erroredAt)
	}

	expectedLeftErrorAt := []time.Duration{7 * time.Second, 14 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}
}

func TestSchedulerCrashedWithoutLeaveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	schedulerNotRecoveredErr := newSchedulerNotRecovered()

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF},
				error:      schedulerNotRecoveredErr,
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      schedulerNotRecoveredErr,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != schedulerNotRecoveredErr.Error() {
		t.Fatalf("Scheduler should have finished with error message \"%v\", but got \"%v\"", schedulerNotRecoveredErr, scheduler.Err)
	}
}

func TestSchedulerCrashedFromPending(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP},
			},
			{
				at:         2,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esX, _esP},
				error:      errors.New("Boom!"),
			},
			{
				at:         3,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esX, _esP},
				error:      errors.New("Boom!"),
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 4 * time.Second,
		},
	)

	expectedPreparedAt := []time.Duration{2 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	expectedCrashedAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerCrashedFromError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 3, kind: Parallel, priority: 0, handler: errorHandler(2), errorHandler: errorHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(2 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 5 * time.Second,
		},
	)

	expectedLeftErrorAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	expectedCrashedAt := []time.Duration{7 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerCrashedFromErrorTwice(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 3, kind: Parallel, priority: 0, handler: errorHandler(2), errorHandler: errorHandler(2)},
			{delay: 4, kind: Parallel, priority: 0, handler: errorHandler(2), errorHandler: errorHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Exploded #%d!", len(errorAt)))
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esR, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS},
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         7,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         8,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esX},
				error:      errors.New("Exploded #1!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
			2: 3 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
			2: 5 * time.Second,
			3: 5 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{8 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Exploded #1!" {
		t.Fatalf("Scheduler should have finished with error message \"Exploded #1!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerCrashedWaitingRunning(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(6, nil), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Exploded #%d!", len(errorAt)))
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(2 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         7,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         9,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Exploded #1!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{6 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Exploded #1!" {
		t.Fatalf("Scheduler should have finished with error message \"Exploded #1!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerCrashedWaitingRunningError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 3, kind: Parallel, priority: 0, handler: errorHandler(5), errorHandler: errorHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: errorHandler(2), errorHandler: errorHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Exploded #%d!", len(errorAt)))
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esR, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS},
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         7,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         8,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esX},
				error:      errors.New("Exploded #1!"),
			},
			{
				at:         9,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esX},
				error:      errors.New("Exploded #1!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
			2: 3 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
			2: 8 * time.Second,
			3: 5 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{8 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Exploded #1!" {
		t.Fatalf("Scheduler should have finished with error message \"Exploded #1!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerCrashedFromErrorWithOnError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(2), errorHandler: errorHandler(2)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(7, nil), errorHandler: testDummyHandler()},
			{delay: 9, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP},
			},
			{
				at:         5,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         6,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         7,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         8,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
				error:      errors.New("Boom!"),
			},
			{
				at:         9,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         10,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         11,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 3 * time.Second,
			2: 9 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{8 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{11 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerCrashedFromClosing(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         2,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         3,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esS, _esP},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esX, _esP},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{
			0: 4 * time.Second,
			1: 5 * time.Second,
		},
	)

	expectedClosingAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	expectedCrashedAt := []time.Duration{7 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerAllInactiveTransitions(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         5,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 3 * time.Second,
		},
		map[int]time.Duration{},
	)

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerClosed(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closedAt := []time.Duration{}
	options.OnClose = func(scheduler *Scheduler[any]) {
		closedAt = append(closedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         4,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			1: 5 * time.Second,
		},
	)

	expectedClosedAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(closedAt, expectedClosedAt) {
		t.Fatalf("OnClose should have finished at %v, but was finished at %v", expectedClosedAt, closedAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerAllActiveTransitions(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDummyHandler()},
			{delay: 8, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 11, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esF, _esR, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esP, _esP},
			},
			{
				at:         5,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP, _esP},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP, _esP},
			},
			{
				at:         7,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP, _esP},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esP},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esP},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esP},
			},
			{
				at:         11,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR},
			},
			{
				at:         12,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         13,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         14,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         15,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         16,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         17,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         18,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 1 * time.Second,
			2: 2 * time.Second,
			3: 8 * time.Second,
			4: 11 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
		},
	)

	expectedLeftErrorAt := []time.Duration{6 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeftError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	expectedClosingAt := []time.Duration{18 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerFromClosingToCrashed(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		if len(closingAt) > 1 {
			return errors.New("Boom!")
		}

		return nil
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         4,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         5,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         6,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         8,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         10,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         11,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         12,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         13,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom!"),
			},
			{
				at:         14,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 7 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedClosingAt := []time.Duration{7 * time.Second, 13 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	expectedCrashedAt := []time.Duration{14 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerFromClosingToClosed(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         4,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         5,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         6,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         8,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         10,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         11,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         12,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         13,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 7 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedClosingAt := []time.Duration{7 * time.Second, 13 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerWaitForWaitGroup(t *testing.T) {
	var waitGroup sync.WaitGroup
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, &waitGroup)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDummyHandler()},
		},
	)

	startedAt := scheduler.clock.Now()
	var finishedAt time.Duration
	go func() {
		waitGroup.Wait()
		finishedAt = scheduler.clock.Since(startedAt)
	}()

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         5,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedFinishedAt := 6 * time.Second
	if expectedFinishedAt != finishedAt {
		t.Fatalf("Scheduler should have finished at %v, but did at %v", expectedFinishedAt, finishedAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerShutdownTwice(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(4, nil), errorHandler: testDummyHandler()},
		},
	)

	scheduler.getClock().AfterFunc(2*time.Second, scheduler.Shutdown)
	scheduler.getClock().AfterFunc(3*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esR},
				error:      shutdownError,
			},
			{
				at:         3,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esR},
				error:      shutdownError,
			},
			{
				at:         4,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esR},
				error:      shutdownError,
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{},
	)

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnPending(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return nil
	}

	scheduler.getClock().AfterFunc(2*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS, _esP},
			},
			{
				at:         2,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esX, _esP},
				error:      shutdownError,
			},
			{
				at:         3,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      shutdownError,
			},
			{
				at:         4,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esX},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
		},
	)

	expectedPreparedAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnActive(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDummyHandler()},
		},
	)
	scheduler.getClock().AfterFunc(2*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esR, _esP},
				error:      shutdownError,
			},
			{
				at:         3,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esR, _esX},
				error:      shutdownError,
			},
			{
				at:         4,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			1: 3 * time.Second,
		},
	)

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnInactive(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	scheduler.getClock().AfterFunc(3*time.Second, scheduler.Shutdown)
	scheduler.getClock().AfterFunc(4*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         3,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      shutdownError,
			},
			{
				at:         4,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      shutdownError,
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{},
	)

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnClosing(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	scheduler.getClock().AfterFunc(6*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         4,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         5,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         6,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      shutdownError,
			},
			{
				at:         7,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      shutdownError,
			},
			{
				at:         8,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			1: 6 * time.Second,
		},
	)

	expectedClosingAt := []time.Duration{8 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnCrashingWhileRunningExecutions(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(4, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	scheduler.getClock().AfterFunc(4*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esP},
			},
			{
				at:         4,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX, _esP},
				error:      shutdownError,
			},
			{
				at:         5,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX, _esX},
				error:      shutdownError,
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX, _esX},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			2: 4 * time.Second,
			3: 5 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnCrashingWhileCallbackRunning(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	scheduler.getClock().AfterFunc(5*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esP},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX},
				error:      shutdownError,
			},
			{
				at:         6,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX},
				error:      shutdownError,
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 4 * time.Second,
			2: 5 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{7 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnErrorWhileRunning(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(4, nil), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	scheduler.getClock().AfterFunc(5*time.Second, scheduler.Shutdown)
	shutdownError := newShutdownError()

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         5,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
				error:      shutdownError,
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      shutdownError,
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			2: 5 * time.Second,
		},
	)

	if scheduler.Err != shutdownError {
		t.Fatalf("Scheduler should have finished with error %v, but got %v", shutdownError, scheduler.Err)
	}
}

func TestSchedulerShutdownOnErrorWhileCallbackRunning(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 6, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	scheduler.getClock().AfterFunc(5*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP, _esP},
			},
			{
				at:         5,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esP},
				error:      shutdownError,
			},
			{
				at:         6,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esX},
				error:      shutdownError,
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 5 * time.Second,
			2: 5 * time.Second,
			3: 6 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{7 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerShutdownOnErrorWhileLeaveCallbackRunning(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 6, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(4 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	scheduler.getClock().AfterFunc(5*time.Second, scheduler.Shutdown)

	shutdownError := newShutdownError()
	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP, _esP},
			},
			{
				at:         5,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esP},
				error:      shutdownError,
			},
			{
				at:         6,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esX},
				error:      shutdownError,
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 5 * time.Second,
			2: 5 * time.Second,
			3: 6 * time.Second,
		},
	)

	expectedLeftErrorAt := []time.Duration{7 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerSerialExecutionsRunsSerially(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 4, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         11,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 3 * time.Second,
			2: 5 * time.Second,
			3: 7 * time.Second,
		},
		map[int]time.Duration{},
	)
}

func TestSchedulerSerialExecutionsRespectsPriority(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 0, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Serial, priority: 4, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Serial, priority: 3, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Serial, priority: 2, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Serial, priority: 1, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esS, _esS},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS, _esS},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS, _esS},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR},
			},
			{
				at:         9,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         11,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         12,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 0 * time.Second,
			1: 2 * time.Second,
			2: 4 * time.Second,
			3: 6 * time.Second,
			4: 8 * time.Second,
		},
		map[int]time.Duration{},
	)
}

func TestSchedulerSerialExecutionExpireWhileRunning(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Serial, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Serial, priority: 3, handler: testDelayedHandler(2, nil), errorHandler: testDelayedHandler(1, errors.New("Boom!"))},
			{delay: 4, kind: Serial, priority: 4, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esX, _esS, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esR},
			},
			{
				at:         7,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esR},
			},
			{
				at:         8,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
				error:      newSchedulerNotRecovered(),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			3: 6 * time.Second,
		},
		map[int]time.Duration{
			1: 5 * time.Second,
			2: 6 * time.Second,
		},
	)
}

func TestSchedulerSerialExecutionLeaveExpiredToInactive(t *testing.T) {
	options := defaultSchedulerOptions()
	options.ExecutionTimeout = 3 * time.Second
	options.InactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Serial, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Serial, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Serial, priority: 2, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(4, nil)},
			{delay: 4, kind: Serial, priority: 3, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esX, _esS, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esR},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esR},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
			},
			{
				at:         9,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
			},
			{
				at:         11,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
			},
			{
				at:         12,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
			},
			{
				at:         13,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			3: 6 * time.Second,
		},
		map[int]time.Duration{
			1: 5 * time.Second,
			2: 6 * time.Second,
		},
	)
}

func TestSchedulerSerialExecutionDuringError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 3 * time.Second
	errorHandler := testErrorHandlerBuilder()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 1, kind: Serial, priority: 0, handler: testDelayedHandler(4, nil), errorHandler: testDummyHandler()},
			{delay: 7, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 7, kind: Serial, priority: 0, handler: errorHandler(3), errorHandler: errorHandler(1)},
			{delay: 14, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 14, kind: Serial, priority: 0, handler: testDelayedHandler(4, nil), errorHandler: testDummyHandler()},
			{delay: 16, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 24, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 24, kind: Serial, priority: 0, handler: testDelayedHandler(4, nil), errorHandler: testDummyHandler()},
			{delay: 25, kind: Serial, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         5,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         6,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         7,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         9,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         10,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         11,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         12,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         13,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         14,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esS, _esS, _esP, _esP, _esP, _esP},
			},
			{
				at:         15,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR, _esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         16,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR, _esR, _esS, _esP, _esP, _esP},
			},
			{
				at:         17,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esR, _esS, _esP, _esP, _esP},
			},
			{
				at:         18,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esR, _esS, _esP, _esP, _esP},
			},
			{
				at:         19,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esP, _esP, _esP},
			},
			{
				at:         20,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esP, _esP, _esP},
			},
			{
				at:         21,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esP, _esP, _esP},
			},
			{
				at:         22,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esP, _esP, _esP},
			},
			{
				at:         23,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esP, _esP, _esP},
			},
			{
				at:         24,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esR, _esR, _esP},
			},
			{
				at:         25,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esR, _esR, _esS},
			},
			{
				at:         26,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esR, _esS},
			},
			{
				at:         27,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esR, _esS},
			},
			{
				at:         28,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
			{
				at:         29,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
			{
				at:         30,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
			{
				at:         31,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
			{
				at:         32,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
			{
				at:         33,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
			{
				at:         34,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esX, _esF, _esF, _esX},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 1 * time.Second,
			2: 8 * time.Second,
			3: 8 * time.Second,
			4: 15 * time.Second,
			5: 15 * time.Second,
			7: 24 * time.Second,
			8: 24 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			2: 9 * time.Second,
			3: 11 * time.Second,
			4: 16 * time.Second,
			6: 19 * time.Second,
			7: 25 * time.Second,
			9: 28 * time.Second,
		},
	)

	expectedLeftError := []time.Duration{8 * time.Second, 15 * time.Second, 23 * time.Second, 32 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftError) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftError, leftErrorAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerSerialFinishWhileCrashed(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 3 * time.Second
	errorHandler := testErrorHandlerBuilder()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 0, kind: Serial, priority: 0, handler: testDelayedHandler(6, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 1, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esR, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP, _esS},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esS, _esS},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 0 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			2: 4 * time.Second,
			3: 4 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerSerialErrorsWhileCrashed(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 3 * time.Second
	errorHandler := testErrorHandlerBuilder()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 1, kind: Serial, priority: 0, handler: errorHandler(3), errorHandler: errorHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 4 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerCrashedWaitsForSerialExpirationFinish(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 2 * time.Second
	errorHandler := testErrorHandlerBuilder()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 0, kind: Serial, priority: 0, handler: errorHandler(4), errorHandler: errorHandler(2)},
			{delay: 1, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: testDelayedHandler(5, nil)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esR, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esS},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esS},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         7,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         8,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 0 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 4 * time.Second,
			2: 3 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerCrashedWaitsForSerialExpirationError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 2 * time.Second
	errorHandler := testErrorHandlerBuilder()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
			{delay: 0, kind: Serial, priority: 0, handler: errorHandler(4), errorHandler: errorHandler(2)},
			{delay: 1, kind: Serial, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(5)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esR, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esS},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esS},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esR, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         7,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         8,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 0 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 4 * time.Second,
			2: 3 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got \"%v\"", scheduler.Err)
	}
}

func TestSchedulerCriticalExecutionsRunsSerially(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Critical, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Critical, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Critical, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 4, kind: Critical, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         11,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 3 * time.Second,
			2: 5 * time.Second,
			3: 7 * time.Second,
		},
		map[int]time.Duration{},
	)
}

func TestSchedulerCriticalExecutionsRespectsPriority(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 0, kind: Critical, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Critical, priority: 4, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Critical, priority: 3, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Critical, priority: 2, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 1, kind: Critical, priority: 1, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esS, _esS},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS, _esS},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS, _esS},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esS, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esS},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR},
			},
			{
				at:         9,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esR},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         11,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         12,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 0 * time.Second,
			1: 2 * time.Second,
			2: 4 * time.Second,
			3: 6 * time.Second,
			4: 8 * time.Second,
		},
		map[int]time.Duration{},
	)
}

func TestSchedulerCriticalExecutionsSchedulesAndWaitsParallels(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Critical, priority: 4, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Parallel, priority: 2, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Parallel, priority: 1, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 2, kind: Critical, priority: 3, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 3, kind: Critical, priority: 2, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
			{delay: 4, kind: Critical, priority: 1, handler: testDelayedHandler(2, nil), errorHandler: testDummyHandler()},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esS, _esS, _esS, _esS, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esS, _esS, _esS, _esR, _esS, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esS, _esS, _esS, _esR, _esS, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esS, _esS, _esS, _esF, _esR, _esS},
			},
			{
				at:         6,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esS, _esS, _esS, _esF, _esR, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS, _esF, _esF, _esS},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esS, _esS, _esF, _esF, _esS},
			},
			{
				at:         9,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esF, _esF, _esR},
			},
			{
				at:         10,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS, _esF, _esF, _esR},
			},
			{
				at:         11,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esF, _esF, _esF},
			},
			{
				at:         12,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR, _esF, _esF, _esF},
			},
			{
				at:         13,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         14,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esF},
			},
			{
				at:         15,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 7 * time.Second,
			2: 11 * time.Second,
			3: 11 * time.Second,
			4: 3 * time.Second,
			5: 5 * time.Second,
			6: 9 * time.Second,
		},
		map[int]time.Duration{},
	)
}

func TestSchedulerHandlerPanic(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testPanicHandler(1, "Boom!"), errorHandler: testDummyHandler()},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(1, errors.New("Boom!")), errorHandler: testPanicHandler(1, "Boom!")},
			{delay: 8, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testPanicHandler(1, "Boom!")},
		},
	)

	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         4,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         5,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         8,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
			},
			{
				at:         9,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esX},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 2 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 3 * time.Second,
			2: 8 * time.Second,
		},
	)

	expectedLeftErrorAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}
}

func TestSchedulerOnInactiveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 6, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 12, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 19, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	startedAt := scheduler.clock.Now()

	inactiveAt := []time.Duration{}
	options.OnInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(time.Duration(1+len(inactiveAt)) * time.Second)
		inactiveAt = append(inactiveAt, scheduler.clock.Since(startedAt))

		if len(inactiveAt) >= 5 {
			return errors.New("Its enough!")
		}

		return nil
	}

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         5,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         6,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP, _esP},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP, _esP},
			},
			{
				at:         8,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP},
			},
			{
				at:         11,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP},
			},
			{
				at:         12,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esP},
			},
			{
				at:         13,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esP},
			},
			{
				at:         14,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP},
			},
			{
				at:         15,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP},
			},
			{
				at:         16,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP},
			},
			{
				at:         17,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP},
			},
			{
				at:         18,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esP},
			},
			{
				at:         19,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esS},
			},
			{
				at:         20,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR},
			},
			{
				at:         21,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         22,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         23,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         24,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         25,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         26,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
				error:      errors.New("Its enough!"),
			},
			{
				at:         27,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
				error:      errors.New("Its enough!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 7 * time.Second,
			2: 13 * time.Second,
			3: 20 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedInactiveAt := []time.Duration{
		1 * time.Second,
		4 * time.Second,
		11 * time.Second,
		18 * time.Second,
		26 * time.Second,
	}
	if !reflect.DeepEqual(inactiveAt, expectedInactiveAt) {
		t.Fatalf("OnInactive should have finished at %v, but was finished at %v", expectedInactiveAt, inactiveAt)
	}

	expectedClosingAt := []time.Duration{
		7 * time.Second,
		13 * time.Second,
		20 * time.Second,
	}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	expectedCrashedAt := []time.Duration{27 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerShutdownOnInactiveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	startedAt := scheduler.clock.Now()

	inactiveAt := []time.Duration{}
	options.OnInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(3 * time.Second)
		inactiveAt = append(inactiveAt, scheduler.clock.Since(startedAt))

		return nil
	}

	scheduler.getClock().AfterFunc(5*time.Second, scheduler.Shutdown)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         5,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF},
				error:      newShutdownError(),
			},
			{
				at:         6,
				status:     ShutdownStatus,
				executions: []testExecutionStatus{_esF},
				error:      newShutdownError(),
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newShutdownError(),
			},
		},
		map[int]time.Duration{0: 3 * time.Second},
		map[int]time.Duration{},
	)

	expectedInactiveAt := []time.Duration{3 * time.Second, 7 * time.Second}
	if !reflect.DeepEqual(inactiveAt, expectedInactiveAt) {
		t.Fatalf("OnInactive should have finished at %v, but was finished at %v", expectedInactiveAt, inactiveAt)
	}
}

func TestSchedulerTimeoutWhileOnInactiveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(0)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 8, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(4)},
		},
	)

	startedAt := scheduler.clock.Now()

	inactiveAt := []time.Duration{}
	options.OnInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(5 * time.Second)
		inactiveAt = append(inactiveAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esP, _esP},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esS, _esP},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esR, _esP},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esP},
			},
			{
				at:         7,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esP},
			},
			{
				at:         8,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esS},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esS},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         11,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         12,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         13,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         14,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         15,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         16,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         17,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         18,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         19,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
			{
				at:         20,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX},
			},
		},
		map[int]time.Duration{1: 5 * time.Second},
		map[int]time.Duration{
			0: 3 * time.Second,
			2: 10 * time.Second,
		},
	)

	expectedInactiveAt := []time.Duration{5 * time.Second, 11 * time.Second, 20 * time.Second}
	if !reflect.DeepEqual(inactiveAt, expectedInactiveAt) {
		t.Fatalf("OnInactive should have finished at %v, but was finished at %v", expectedInactiveAt, inactiveAt)
	}

	expectedLeftErrorAt := []time.Duration{15 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}
}

func TestSchedulerTimeoutAfterOnInactiveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 5 * time.Second
	options.ExecutionTimeout = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(0)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 8, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(4)},
		},
	)

	startedAt := scheduler.clock.Now()

	inactiveAt := []time.Duration{}
	options.OnInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		inactiveAt = append(inactiveAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         7,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR},
			},
			{
				at:         9,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         11,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         12,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         13,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
			{
				at:         14,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 5 * time.Second,
			2: 8 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedInactiveAt := []time.Duration{
		2 * time.Second,
		5 * time.Second,
		8 * time.Second,
		11 * time.Second,
	}
	if !reflect.DeepEqual(inactiveAt, expectedInactiveAt) {
		t.Fatalf("OnInactive should have finished at %v, but was finished at %v", expectedInactiveAt, inactiveAt)
	}
}

func TestSchedulerOnLeaveInactiveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 9, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 12, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 13, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	startedAt := scheduler.clock.Now()

	leftInactiveAt := []time.Duration{}
	options.OnLeaveInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(time.Duration(1+len(leftInactiveAt)) * time.Second)
		leftInactiveAt = append(leftInactiveAt, scheduler.clock.Since(startedAt))

		if len(leftInactiveAt) >= 4 {
			return errors.New("Its enough!")
		}

		return nil
	}

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(2 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         5,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         7,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         8,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP, _esP},
			},
			{
				at:         9,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP, _esP},
			},
			{
				at:         10,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR, _esP, _esP},
			},
			{
				at:         11,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esP, _esP},
			},
			{
				at:         12,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esP},
			},
			{
				at:         13,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS},
			},
			{
				at:         14,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esS, _esS},
			},
			{
				at:         15,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esR, _esR},
			},
			{
				at:         16,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         17,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         18,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         19,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         20,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         21,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         22,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
			},
			{
				at:         23,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
				error:      errors.New("Its enough!"),
			},
			{
				at:         24,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF},
				error:      errors.New("Its enough!"),
			},
		},
		map[int]time.Duration{
			0: 2 * time.Second,
			1: 10 * time.Second,
			2: 15 * time.Second,
			3: 15 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedLeftInactiveAt := []time.Duration{
		2 * time.Second,
		8 * time.Second,
		15 * time.Second,
		23 * time.Second,
	}
	if !reflect.DeepEqual(leftInactiveAt, expectedLeftInactiveAt) {
		t.Fatalf("OnLeaveInactive should have finished at %v, but was finished at %v", expectedLeftInactiveAt, leftInactiveAt)
	}

	expectedClosingAt := []time.Duration{10 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	expectedCrashedAt := []time.Duration{24 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerTimeoutOnLeaveInactiveCallback(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	options.ExecutionTimeout = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 4, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: errorHandler(3)},
			{delay: 6, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	startedAt := scheduler.clock.Now()

	leftInactiveAt := []time.Duration{}
	options.OnLeaveInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(5 * time.Second)
		leftInactiveAt = append(leftInactiveAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esS, _esS, _esS, _esP},
			},
			{
				at:         5,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esS, _esS, _esP},
			},
			{
				at:         6,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esS},
			},
			{
				at:         7,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esR},
			},
			{
				at:         8,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         9,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         10,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         11,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         12,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         13,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         14,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         15,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         16,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
			{
				at:         17,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esX, _esX, _esX, _esF},
			},
		},
		map[int]time.Duration{3: 7 * time.Second},
		map[int]time.Duration{
			0: 5 * time.Second,
			1: 6 * time.Second,
			2: 6 * time.Second,
		},
	)

	expectedLeftInactiveAt := []time.Duration{7 * time.Second, 17 * time.Second}
	if !reflect.DeepEqual(leftInactiveAt, expectedLeftInactiveAt) {
		t.Fatalf("OnLeaveInactive should have finished at %v, but was finished at %v", expectedLeftInactiveAt, leftInactiveAt)
	}

	expectedLeftErrorAt := []time.Duration{10 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}
}

func TestSchedulerHandlePanicOnPrepare(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{},
	)

	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.OnPrepare = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		panic("Something went really bad, but I am recovered.")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{},
			},
			{
				at:         1,
				status:     CrashedStatus,
				executions: []testExecutionStatus{},
				error:      newPanicError("OnPrepare", "Something went really bad, but I am recovered."),
			},
			{
				at:         2,
				status:     ClosedStatus,
				executions: []testExecutionStatus{},
				error:      newPanicError("OnPrepare", "Something went really bad, but I am recovered."),
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{},
	)

	expectedPreparedAt := []time.Duration{1 * time.Second}
	if !reflect.DeepEqual(preparedAt, expectedPreparedAt) {
		t.Fatalf("OnPrepare should have finished at %v, but was finished at %v", expectedPreparedAt, preparedAt)
	}

	expectedCrashedAt := []time.Duration{2 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerHandlePanicOnClosing(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)

	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.OnClosing = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		panic("Something went really bad, but I am recovered.")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         3,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         4,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newPanicError("OnClosing", "Something went really bad, but I am recovered."),
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newPanicError("OnClosing", "Something went really bad, but I am recovered."),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedClosingAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	expectedCrashedAt := []time.Duration{6 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerHandlePanicOnError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)

	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.OnError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		panic("Something went really bad, but I am recovered.")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newPanicError("OnError", "Something went really bad, but I am recovered."),
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newPanicError("OnError", "Something went really bad, but I am recovered."),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
		},
	)

	expectedErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(errorAt, expectedErrorAt) {
		t.Fatalf("OnError should have finished at %v, but was finished at %v", expectedErrorAt, errorAt)
	}

	expectedCrashedAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerHandlePanicOnLeaveError(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	errorHandler := testErrorHandlerBuilder()
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: errorHandler(1), errorHandler: errorHandler(1)},
		},
	)

	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.OnLeaveError = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		panic("Something went really bad, but I am recovered.")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         3,
				status:     ErrorStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newPanicError("OnLeaveError", "Something went really bad, but I am recovered."),
			},
			{
				at:         5,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
				error:      newPanicError("OnLeaveError", "Something went really bad, but I am recovered."),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			0: 2 * time.Second,
		},
	)

	expectedLeftErrorAt := []time.Duration{4 * time.Second}
	if !reflect.DeepEqual(leftErrorAt, expectedLeftErrorAt) {
		t.Fatalf("OnLeaveError should have finished at %v, but was finished at %v", expectedLeftErrorAt, leftErrorAt)
	}

	expectedCrashedAt := []time.Duration{5 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerHandlePanicOnInactive(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{},
	)

	startedAt := scheduler.clock.Now()

	inactiveAt := []time.Duration{}
	options.OnInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		inactiveAt = append(inactiveAt, scheduler.clock.Since(startedAt))

		panic("Something went really bad, but I am recovered.")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{},
			},
			{
				at:         1,
				status:     CrashedStatus,
				executions: []testExecutionStatus{},
				error:      newPanicError("OnInactive", "Something went really bad, but I am recovered."),
			},
			{
				at:         2,
				status:     ClosedStatus,
				executions: []testExecutionStatus{},
				error:      newPanicError("OnInactive", "Something went really bad, but I am recovered."),
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{},
	)

	expectedInactiveAt := []time.Duration{1 * time.Second}
	if !reflect.DeepEqual(inactiveAt, expectedInactiveAt) {
		t.Fatalf("OnInactive should have finished at %v, but was finished at %v", expectedInactiveAt, inactiveAt)
	}

	expectedCrashedAt := []time.Duration{2 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}

func TestSchedulerHandlePanicOnLeaveInactive(t *testing.T) {
	options := defaultSchedulerOptions()
	options.InactivityDelay = 1 * time.Second
	scheduler := NewScheduler(options, nil, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{},
	)

	startedAt := scheduler.clock.Now()

	leftInactiveAt := []time.Duration{}
	options.OnLeaveInactive = func(scheduler *Scheduler[any]) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftInactiveAt = append(leftInactiveAt, scheduler.clock.Since(startedAt))

		panic("Something went really bad, but I am recovered.")
	}

	crashedAt := []time.Duration{}
	options.OnCrash = func(scheduler *Scheduler[any]) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     InactiveStatus,
				executions: []testExecutionStatus{},
			},
			{
				at:         1,
				status:     InactiveStatus,
				executions: []testExecutionStatus{},
			},
			{
				at:         2,
				status:     CrashedStatus,
				executions: []testExecutionStatus{},
				error:      newPanicError("OnLeaveInactive", "Something went really bad, but I am recovered."),
			},
			{
				at:         3,
				status:     ClosedStatus,
				executions: []testExecutionStatus{},
				error:      newPanicError("OnLeaveInactive", "Something went really bad, but I am recovered."),
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{},
	)

	expectedLeftInactiveAt := []time.Duration{2 * time.Second}
	if !reflect.DeepEqual(leftInactiveAt, expectedLeftInactiveAt) {
		t.Fatalf("OnLeaveInactive should have finished at %v, but was finished at %v", expectedLeftInactiveAt, leftInactiveAt)
	}

	expectedCrashedAt := []time.Duration{3 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrash should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}
}
