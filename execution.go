package goroutine_scheduler

import "errors"

type ExecutionStatus uint64

const (
	ExecutionPending ExecutionStatus = iota
	ExecutionRunning
	ExecutionExpired
	ExecutionFinished
)

type Execution struct {
	status       ExecutionStatus
	handler      func() error
	errorHandler func(error) error
	priority     int
	kind         ExecutionKind
	index        int
}

func NewExecution(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
	return &Execution{
		status:       ExecutionPending,
		handler:      handler,
		errorHandler: errorHandler,
		priority:     priority,
		kind:         kind,
		index:        -1,
	}
}

func (execution *Execution) call(scheduler *Scheduler) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if execution.status == ExecutionPending {
		execution.status = ExecutionRunning
		go execution.run(scheduler)
	}
}

func (execution *Execution) expire(scheduler *Scheduler) {
	scheduler.lock.Lock()
	defer scheduler.lock.Unlock()

	if execution.status == ExecutionPending {
		execution.status = ExecutionExpired
		if execution.kind == Parallel {
			scheduler.parallelQueue.Remove(execution)
		} else {
			scheduler.serialQueue.Remove(execution)
		}
		go execution.errorHandler(errors.New("Timeout error"))
	}
}

func (execution *Execution) run(scheduler *Scheduler) {
	err := execution.handler()

	if err != nil {
		err = execution.errorHandler(err)
	}

	if err != nil {
		scheduler.events <- ErrorEvent
	} else {
		scheduler.events <- FinishedEvent
	}

	scheduler.lock.Lock()
	execution.status = ExecutionFinished
	scheduler.lock.Unlock()
}
