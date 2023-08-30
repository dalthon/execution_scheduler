package execution_scheduler

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
