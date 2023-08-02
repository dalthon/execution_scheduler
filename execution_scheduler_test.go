package execution_scheduler

import (
	"errors"
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

// TODO: Handle error going back to Pending
func TestSchedulerOnPrepareTimeline(t *testing.T) {
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
	var preparedAt time.Duration
	options.onPrepare = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = scheduler.clock.Since(startedAt)

		return nil
	}

	timeline.expects(
		[]testTimelineExpectations{
			{
				at:         0,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esP},
			},
			{
				at:         1,
				status:     PendingStatus,
				executions: []testExecutionStatus{_esS},
			},
			{
				at:         2,
				status:     ActiveStatus,
				executions: []testExecutionStatus{_esR},
			},
			{
				at:         3,
				status:     ClosedStatus,
				executions: []testExecutionStatus{_esF},
			},
		},
		map[int]time.Duration{
			0: 2 * time.Second,
		},
		map[int]time.Duration{},
	)

	if preparedAt != 2*time.Second {
		t.Fatalf("OnPrepare should have finished at 2s, but was finished at %v", preparedAt)
	}
}

func TestSchedulerCrashedOnPrepareTimeline(t *testing.T) {
	options := defaultSchedulerOptions()
	scheduler := NewScheduler(options, nil)
	timeline := newTestTimelinesExample(
		t,
		scheduler,
		[]testTimelineParams{},
	)

	startedAt := scheduler.clock.Now()
	var preparedAt time.Duration
	options.onPrepare = func(scheduler *Scheduler) error {
		scheduler.clock.Sleep(2 * time.Second)
		preparedAt = scheduler.clock.Since(startedAt)

		return errors.New("Crashed on onPrepare")
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
				status:     PendingStatus,
				executions: []testExecutionStatus{},
			},
			{
				at:         2,
				status:     CrashedStatus,
				executions: []testExecutionStatus{},
			},
		},
		map[int]time.Duration{},
		map[int]time.Duration{},
	)

	if preparedAt != 2*time.Second {
		t.Fatalf("OnPrepare should have finished at 2s, but was finished at %v", preparedAt)
	}
}

// TODO: Handle error going back to Pending
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

func TestSchedulerCrashedOnClosingTimeline(t *testing.T) {
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

		return errors.New("Crashed on onClosing")
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
				status:     CrashedStatus,
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
