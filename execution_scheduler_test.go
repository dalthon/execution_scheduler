package execution_scheduler

import (
	"errors"
	"fmt"
	"reflect"
	"time"

	"testing"
)

func TestSchedulerEmptyTimeline(t *testing.T) {
	scheduler := NewScheduler(defaultSchedulerOptions(), nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{at: 0, status: ActiveStatus, executions: []testExecutionStatus{}},
		},
		map[int]time.Duration{},
		map[int]time.Duration{},
	)
}

func TestSchedulerTimeout(t *testing.T) {
	options := defaultSchedulerOptions()
	options.executionTimeout = 2 * time.Second
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func() testDelayedHandlerParams {
		handler := testDelayedHandler(1, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
			{delay: 8, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 16, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 18, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.onPrepare = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(4 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(4 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	closingAt := []time.Duration{}
	options.onClosing = func(scheduler *Scheduler) error {
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
				status:     ActiveStatus,
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
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esP, _esP},
			},
			{
				at:         16,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esR, _esP},
			},
			{
				at:         17,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esP},
			},
			{
				at:         18,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esS},
			},
			{
				at:         19,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esS},
			},
			{
				at:         20,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esX, _esF, _esX, _esF, _esX},
			},
			{
				at:         21,
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
			4: 20 * time.Second,
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

	expectedClosingAt := []time.Duration{21 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

func TestSchedulerAllPendingTransitions(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func() testDelayedHandlerParams {
		handler := testDelayedHandler(1, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
			{delay: 6, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.onPrepare = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		if len(preparedAt) == 3 {
			return errors.New("Its enough!")
		}

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	options.onCrash = func(scheduler *Scheduler) {
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
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         11,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         12,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Its enough!"),
			},
			{
				at:         13,
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

	expectedPreparedAt := []time.Duration{2 * time.Second, 7 * time.Second, 12 * time.Second}
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

func TestSchedulerAllErrorTransitions(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func() testDelayedHandlerParams {
		handler := testDelayedHandler(1, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
			{delay: 6, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	preparedAt := []time.Duration{}
	options.onPrepare = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		if len(leftErrorAt) == 2 {
			return errors.New("Its enough!")
		}

		return nil
	}

	options.onCrash = func(scheduler *Scheduler) {
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func() testDelayedHandlerParams {
		handler := testDelayedHandler(1, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
			{delay: 2, kind: Parallel, priority: 0, handler: blownUpHandler(), errorHandler: blownUpHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Boom #%d!", len(errorAt)))
	}

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(1 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return errors.New("Should not be here!")
	}

	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(1 * time.Second)
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(defaultSchedulerOptions(), nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
				error:      NewSchedulerNotRecovered(),
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
	scheduler := NewScheduler(defaultSchedulerOptions(), nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: blownUpHandler(2), errorHandler: blownUpHandler(1)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
				error:      NewSchedulerNotRecovered(),
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 4, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 5, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 10, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: blownUpHandler(1)},
			{delay: 13, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: blownUpHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	erroredAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		erroredAt = append(erroredAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 5, kind: Parallel, priority: 0, handler: blownUpHandler(4), errorHandler: blownUpHandler(1)},
			{delay: 10, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: blownUpHandler(1)},
			{delay: 13, kind: Parallel, priority: 0, handler: testDelayedHandler(2, nil), errorHandler: blownUpHandler(1)},
		},
	)
	startedAt := scheduler.clock.Now()

	erroredAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		erroredAt = append(erroredAt, scheduler.clock.Since(startedAt))

		return nil
	}

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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

func TestSchedulerCrashedFromPending(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
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
	options.onPrepare = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = append(preparedAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 3, kind: Parallel, priority: 0, handler: blownUpHandler(2), errorHandler: blownUpHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	leftErrorAt := []time.Duration{}
	options.onLeaveError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		leftErrorAt = append(leftErrorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(2 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 3, kind: Parallel, priority: 0, handler: blownUpHandler(2), errorHandler: blownUpHandler(2)},
			{delay: 4, kind: Parallel, priority: 0, handler: blownUpHandler(2), errorHandler: blownUpHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Exploded #%d!", len(errorAt)))
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(6, nil), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(1 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Exploded #%d!", len(errorAt)))
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(2 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 2, kind: Parallel, priority: 0, handler: blownUpHandler(1), errorHandler: blownUpHandler(1)},
			{delay: 3, kind: Parallel, priority: 0, handler: blownUpHandler(5), errorHandler: blownUpHandler(1)},
			{delay: 4, kind: Parallel, priority: 0, handler: blownUpHandler(2), errorHandler: blownUpHandler(2)},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New(fmt.Sprintf("Exploded #%d!", len(errorAt)))
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	blownHandlerCount := 0
	blownUpHandler := func(delay int) testDelayedHandlerParams {
		handler := testDelayedHandler(delay, errors.New(fmt.Sprintf("Boom %d", blownHandlerCount)))
		blownHandlerCount++
		return handler
	}
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: blownUpHandler(2), errorHandler: blownUpHandler(2)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(7, nil), errorHandler: testDummyHandler()},
			{delay: 9, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	errorAt := []time.Duration{}
	options.onError = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(3 * time.Second)
		errorAt = append(errorAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 5, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.onClosing = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return errors.New("Boom!")
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(3 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP, _esP},
			},
			{
				at:         2,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP, _esP},
			},
			{
				at:         3,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS, _esP},
			},
			{
				at:         4,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esP},
				error:      errors.New("Boom!"),
			},
			{
				at:         5,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         6,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX},
				error:      errors.New("Boom!"),
			},
			{
				at:         7,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX, _esX},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			1: 4 * time.Second,
			2: 5 * time.Second,
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
	options.inactivityDelay = 3 * time.Second
	scheduler := NewScheduler(options, nil)
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
				status:     ActiveStatus,
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
	scheduler := NewScheduler(options, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closedAt := []time.Duration{}
	options.onClose = func(scheduler *Scheduler) {
		closedAt = append(closedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
			},
			{
				at:         4,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esX},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{
			1: 3 * time.Second,
		},
	)

	expectedClosedAt := []time.Duration{2 * time.Second}
	if !reflect.DeepEqual(closedAt, expectedClosedAt) {
		t.Fatalf("OnClose should have finished at %v, but was finished at %v", expectedClosedAt, closedAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

// TODO: Handle Active transitions

func TestSchedulerFromClosingToCrashed(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.onClosing = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(3 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		if len(closingAt) > 1 {
			return errors.New("Boom!")
		}

		return nil
	}

	crashedAt := []time.Duration{}
	options.onCrash = func(scheduler *Scheduler) {
		scheduler.clock.Sleep(1 * time.Second)
		crashedAt = append(crashedAt, scheduler.clock.Since(startedAt))
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         4,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         6,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         7,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         8,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         9,
				status:     CrashedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom!"),
			},
			{
				at:         10,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
				error:      errors.New("Boom!"),
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 5 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedClosingAt := []time.Duration{5 * time.Second, 9 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	expectedCrashedAt := []time.Duration{10 * time.Second}
	if !reflect.DeepEqual(crashedAt, expectedCrashedAt) {
		t.Fatalf("OnCrashed should have finished at %v, but was finished at %v", expectedCrashedAt, crashedAt)
	}

	if scheduler.Err == nil || scheduler.Err.Error() != "Boom!" {
		t.Fatalf("Scheduler should have finished with error message \"Boom!\", but got %v", scheduler.Err)
	}
}

func TestSchedulerFromClosingToClosed(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
			{delay: 3, kind: Parallel, priority: 0, handler: testDummyHandler(), errorHandler: testDummyHandler()},
		},
	)
	startedAt := scheduler.clock.Now()

	closingAt := []time.Duration{}
	options.onClosing = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(3 * time.Second)
		closingAt = append(closingAt, scheduler.clock.Since(startedAt))

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esP},
			},
			{
				at:         2,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esP},
			},
			{
				at:         3,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         4,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esS},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esR},
			},
			{
				at:         6,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         7,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         8,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
			{
				at:         9,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
			1: 5 * time.Second,
		},
		map[int]time.Duration{},
	)

	expectedClosingAt := []time.Duration{5 * time.Second, 9 * time.Second}
	if !reflect.DeepEqual(closingAt, expectedClosingAt) {
		t.Fatalf("OnClosing should have finished at %v, but was finished at %v", expectedClosingAt, closingAt)
	}

	if scheduler.Err != nil {
		t.Fatalf("Scheduler should have finished with no error, but got %v", scheduler.Err)
	}
}

// TODO: Check if tests below still makes sense

func TestSchedulerOnClosingTimeline(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	startedAt := scheduler.clock.Now()
	var closedAt time.Duration
	options.onClosing = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(1 * time.Second)
		closedAt = scheduler.clock.Since(startedAt)

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         2,
				status:     ClosingStatus,
				executions: []testExecutionStatus{_esF},
			},
			{
				at:         3,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
			},
		},
		map[int]time.Duration{
			0: 1 * time.Second,
		},
		map[int]time.Duration{},
	)

	if closedAt != 3*time.Second {
		t.Fatalf("OnClosing should have finished at 3s, but was finished at %v", closedAt)
	}
}

func TestSchedulerInactivityDelayTimeline(t *testing.T) {
	options := defaultSchedulerOptions()
	options.inactivityDelay = 2 * time.Second
	scheduler := NewScheduler(options, nil)

	newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 3, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	).expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
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
}

func TestSchedulerMinimalParallelTimeline(t *testing.T) {
	scheduler := NewScheduler(defaultSchedulerOptions(), nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 0, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{at: 0, status: ActiveStatus, executions: []testExecutionStatus{_esR}},
			{at: 1, status: ClosedStatus, executions: []testExecutionStatus{_esF}},
		},
		map[int]time.Duration{0: 0 * time.Second},
		map[int]time.Duration{},
	)
}

func TestSchedulerMinimalSerialTimeline(t *testing.T) {
	scheduler := NewScheduler(defaultSchedulerOptions(), nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 0, kind: Serial, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{at: 0, status: ActiveStatus, executions: []testExecutionStatus{_esR}},
			{at: 1, status: ClosedStatus, executions: []testExecutionStatus{_esF}},
		},
		map[int]time.Duration{0: 0 * time.Second},
		map[int]time.Duration{},
	)
}

func TestSchedulerTimeline1(t *testing.T) {
	scheduler := NewScheduler(defaultSchedulerOptions(), nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{
			{delay: 0, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 0, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 0, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 1, kind: Parallel, priority: 0, handler: testDelayedHandler(5, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 2, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 3, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 3, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 3, kind: Parallel, priority: 0, handler: testDelayedHandler(3, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
			{delay: 4, kind: Parallel, priority: 0, handler: testDelayedHandler(1, nil), errorHandler: testDelayedHandler(1, nil)},
		},
	)

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         1,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esR, _esR, _esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esP, _esP, _esP, _esP, _esP, _esP},
			},
			{
				at:         3,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esP, _esP, _esP},
			},
			{
				at:         4,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR, _esR},
			},
			{
				at:         5,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esR, _esR, _esR, _esF, _esF, _esF, _esR, _esR, _esR, _esF, _esF, _esF},
			},
			{
				at:         6,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF, _esF},
			},
		},
		map[int]time.Duration{
			0:  0 * time.Second,
			1:  0 * time.Second,
			2:  0 * time.Second,
			3:  1 * time.Second,
			4:  1 * time.Second,
			5:  1 * time.Second,
			6:  2 * time.Second,
			7:  2 * time.Second,
			8:  2 * time.Second,
			9:  3 * time.Second,
			10: 3 * time.Second,
			11: 3 * time.Second,
			12: 4 * time.Second,
			13: 4 * time.Second,
			14: 4 * time.Second,
		},
		map[int]time.Duration{},
	)
}
