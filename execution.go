package execution_scheduler

import (
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
	if execution.Status != ExecutionScheduled {
		return false
	}

	execution.Status = ExecutionRunning
	if execution.timer != nil {
		execution.timer.Stop()
		execution.timer = nil
	}

	scheduler.beforeExecutionCall(execution)
	go execution.run(scheduler)
	return true
}

func (execution *Execution) run(scheduler schedulerInterface) {
	defer execution.recoverFromRunPanic(scheduler)

	err := execution.handler()

	if err != nil {
		err = execution.errorHandler(err)
	}

	execution.Status = ExecutionFinished
	execution.notifyScheduler(scheduler, err, true)
}

func (execution *Execution) recoverFromRunPanic(scheduler schedulerInterface) {
	recovery := recover()
	if recovery == nil {
		return
	}

	execution.Status = ExecutionFinished
	execution.notifyScheduler(scheduler, NewPanicError("Execution", recovery), true)
}

func (execution *Execution) setExpiration(scheduler schedulerInterface, duration time.Duration) {
	execution.timer = scheduler.getClock().AfterFunc(
		duration,
		func() {
			scheduler.getLock().Lock()
			defer scheduler.getLock().Unlock()

			execution.expire(scheduler, NewTimeoutError())
		},
	)
}

// TODO: Think about TimeoutError not changing scheduler status to Error
func (execution *Execution) expire(scheduler schedulerInterface, err error) bool {
	if execution.Status == ExecutionScheduled {
		execution.Status = ExecutionExpired
		if execution.timer != nil {
			execution.timer.Stop()
			execution.timer = nil
		}
		scheduler.remove(execution)

		scheduler.beforeExpireCall(execution)
		go func() {
			defer execution.recoverFromExpirePanic(scheduler)
			err := execution.errorHandler(err)
			execution.notifyScheduler(scheduler, err, false)
		}()
		return true
	}

	return false
}

func (execution *Execution) recoverFromExpirePanic(scheduler schedulerInterface) {
	recovery := recover()
	if recovery == nil {
		return
	}

	execution.notifyScheduler(scheduler, NewPanicError("Execution", recovery), true)
}

func (execution *Execution) notifyScheduler(scheduler schedulerInterface, err error, called bool) {
	if execution.kind == Parallel {
		if err == nil {
			scheduler.signal(FinishedParallelEvent)
		} else {
			scheduler.signal(ErrorParallelEvent)
		}
		return
	}

	if err == nil {
		if called {
			scheduler.signal(FinishedSerialEvent)
		} else {
			scheduler.signal(ExpiredFinishedSerialEvent)
		}
		return
	}

	if called {
		scheduler.signal(ErrorSerialEvent)
	} else {
		scheduler.signal(ExpiredSerialEvent)
	}
}
