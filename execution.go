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

type Execution[C any] struct {
	Status       ExecutionStatus
	handler      func(C) error
	errorHandler func(C, error) error
	priority     int
	kind         ExecutionKind
	timer        clockwork.Timer
	index        int
}

func newExecution[C any](handler func(C) error, errorHandler func(C, error) error, kind ExecutionKind, priority int) *Execution[C] {
	return &Execution[C]{
		Status:       ExecutionScheduled,
		handler:      handler,
		errorHandler: errorHandler,
		priority:     priority,
		kind:         kind,
		timer:        nil,
		index:        -1,
	}
}

func (execution *Execution[C]) call(scheduler schedulerInterface[C]) bool {
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

func (execution *Execution[C]) run(scheduler schedulerInterface[C]) {
	defer execution.recoverFromRunPanic(scheduler)

	err := (panicProofHandler(execution.handler))(scheduler.getContext())

	if err != nil {
		err = execution.errorHandler(scheduler.getContext(), err)
	}

	execution.Status = ExecutionFinished
	execution.notifyScheduler(scheduler, err, true)
}

func (execution *Execution[C]) recoverFromRunPanic(scheduler schedulerInterface[C]) {
	recovery := recover()
	if recovery == nil {
		return
	}

	execution.Status = ExecutionFinished
	execution.notifyScheduler(scheduler, newPanicError("Execution", recovery), true)
}

func (execution *Execution[C]) setExpiration(scheduler schedulerInterface[C], duration time.Duration) {
	execution.timer = scheduler.getClock().AfterFunc(
		duration,
		func() {
			scheduler.getLock().Lock()
			defer scheduler.getLock().Unlock()

			execution.expire(scheduler, newTimeoutError())
		},
	)
}

// TODO: Think about TimeoutError not changing scheduler status to Error
func (execution *Execution[C]) expire(scheduler schedulerInterface[C], err error) bool {
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
			err := execution.errorHandler(scheduler.getContext(), err)
			execution.notifyScheduler(scheduler, err, false)
		}()
		return true
	}

	return false
}

func (execution *Execution[C]) recoverFromExpirePanic(scheduler schedulerInterface[C]) {
	recovery := recover()
	if recovery == nil {
		return
	}

	execution.notifyScheduler(scheduler, newPanicError("Execution", recovery), true)
}

func (execution *Execution[C]) notifyScheduler(scheduler schedulerInterface[C], err error, called bool) {
	if execution.kind == Parallel {
		if err == nil {
			scheduler.signal(finishedParallelEvent)
		} else {
			scheduler.signal(errorParallelEvent)
		}
		return
	}

	if err == nil {
		if called {
			scheduler.signal(finishedSerialEvent)
		} else {
			scheduler.signal(expiredFinishedSerialEvent)
		}
		return
	}

	if called {
		scheduler.signal(errorSerialEvent)
	} else {
		scheduler.signal(expiredSerialEvent)
	}
}

func panicProofHandler[C any](callback func(C) error) func(C) error {
	return func(context C) error {
		errorChannel := make(chan error)

		go func() {
			defer func() {
				if recovery := recover(); recovery != nil {
					errorChannel <- newPanicError("Execution", recovery)
				}
			}()

			err := callback(context)
			errorChannel <- err
		}()

		return <-errorChannel
	}
}
