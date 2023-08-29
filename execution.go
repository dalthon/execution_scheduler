package execution_scheduler

import (
	"errors"
	"time"

	"github.com/jonboulle/clockwork"
)

type ExecutionStatus uint64

const (
	ExecutionScheduled ExecutionStatus = iota
	ExecutionRunning
	ExecutionExpired
	ExecutionFinished
)

type Execution struct {
	Status       ExecutionStatus
	handler      func() error
	errorHandler func(error) error
	priority     int
	kind         ExecutionKind
	timer        clockwork.Timer
	index        int
}

func NewExecution(handler func() error, errorHandler func(error) error, kind ExecutionKind, priority int) *Execution {
	return &Execution{
		Status:       ExecutionScheduled,
		handler:      handler,
		errorHandler: errorHandler,
		priority:     priority,
		kind:         kind,
		timer:        nil,
		index:        -1,
	}
}

func (execution *Execution) call(scheduler schedulerInterface) bool {
	scheduler.getLock().Lock()
	defer scheduler.getLock().Unlock()

	if execution.Status == ExecutionScheduled {
		execution.Status = ExecutionRunning
		if execution.timer != nil {
			execution.timer.Stop()
			execution.timer = nil
		}

		go execution.run(scheduler)
		return true
	}

	return false
}

func (execution *Execution) run(scheduler schedulerInterface) {
	err := execution.handler()

	if err != nil {
		err = execution.errorHandler(err)
	}

	if err == nil {
		scheduler.signal(FinishedEvent)
	} else {
		scheduler.signal(ErrorEvent)
	}

	scheduler.getLock().Lock()
	execution.Status = ExecutionFinished
	scheduler.getLock().Unlock()
}

func (execution *Execution) setExpiration(scheduler schedulerInterface, duration time.Duration) {
	execution.timer = scheduler.getClock().AfterFunc(
		duration,
		func() { execution.expire(scheduler, errors.New("Timeout error")) },
	)
}

// TODO: Create proper Timeout Error
func (execution *Execution) expire(scheduler schedulerInterface, err error) bool {
	scheduler.getLock().Lock()
	defer scheduler.getLock().Unlock()

	if execution.Status == ExecutionScheduled {
		execution.Status = ExecutionExpired
		execution.timer = nil
		scheduler.remove(execution)

		go func() {
			err := execution.errorHandler(err)
			if err == nil {
				scheduler.signal(FinishedEvent)
			} else {
				scheduler.signal(ErrorEvent)
			}
		}()
		return true
	}

	return false
}
