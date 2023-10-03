package execution_scheduler

import (
	"fmt"
)

type TimeoutError struct {
}

func NewTimeoutError() *TimeoutError {
	return &TimeoutError{}
}

func (err *TimeoutError) Error() string {
	return "Execution timed out"
}

type SchedulerCrashedError struct {
}

func NewSchedulerCrashedError() *SchedulerCrashedError {
	return &SchedulerCrashedError{}
}

func (err *SchedulerCrashedError) Error() string {
	return "Cant't schedule on crashed scheduler"
}

type SchedulerNotRecovered struct {
}

func NewSchedulerNotRecovered() *SchedulerNotRecovered {
	return &SchedulerNotRecovered{}
}

func (err *SchedulerNotRecovered) Error() string {
	return "Scheduler could not be recovered from error"
}

type ShutdownError struct {
}

func NewShutdownError() *ShutdownError {
	return &ShutdownError{}
}

func (err *ShutdownError) Error() string {
	return "Scheduler was shut down"
}

type PanicError struct {
	target string
	panic  interface{}
}

func NewPanicError(target string, p interface{}) *PanicError {
	return &PanicError{
		target: target,
		panic:  p,
	}
}

func (err *PanicError) Error() string {
	return fmt.Sprintf("%s panicked with: %v\n", err.target, err.panic)
}
