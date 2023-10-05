package execution_scheduler

import (
	"fmt"
)

type TimeoutError struct {
}

func newTimeoutError() *TimeoutError {
	return &TimeoutError{}
}

func (err *TimeoutError) Error() string {
	return "Execution timed out"
}

type SchedulerCrashedError struct {
}

func newSchedulerCrashedError() *SchedulerCrashedError {
	return &SchedulerCrashedError{}
}

func (err *SchedulerCrashedError) Error() string {
	return "Cant't schedule on crashed scheduler"
}

type SchedulerNotRecovered struct {
}

func newSchedulerNotRecovered() *SchedulerNotRecovered {
	return &SchedulerNotRecovered{}
}

func (err *SchedulerNotRecovered) Error() string {
	return "Scheduler could not be recovered from error"
}

type ShutdownError struct {
}

func newShutdownError() *ShutdownError {
	return &ShutdownError{}
}

func (err *ShutdownError) Error() string {
	return "Scheduler was shut down"
}

type PanicError struct {
	target string
	panic  interface{}
}

func newPanicError(target string, p interface{}) *PanicError {
	return &PanicError{
		target: target,
		panic:  p,
	}
}

func (err *PanicError) Error() string {
	return fmt.Sprintf("%s panicked with: %v\n", err.target, err.panic)
}
