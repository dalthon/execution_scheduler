package execution_scheduler

import "testing"

func TestTimeoutError(t *testing.T) {
	err := newTimeoutError()
	if err.Error() != "Execution timed out" {
		t.Fatalf("TimeoutError should have message \"Execution timed out\", but got \"%v\"", err.Error())
	}
}

func TestSchedulerCrashedError(t *testing.T) {
	err := newSchedulerCrashedError()
	if err.Error() != "Cant't schedule on crashed scheduler" {
		t.Fatalf("TimeoutError should have message \"Cant't schedule on crashed scheduler\", but got \"%v\"", err.Error())
	}
}

func TestSchedulerNotRecovered(t *testing.T) {
	err := newSchedulerNotRecovered()
	if err.Error() != "Scheduler could not be recovered from error" {
		t.Fatalf("TimeoutError should have message \"Scheduler could not be recovered from error\", but got \"%v\"", err.Error())
	}
}

func TestPanicError(t *testing.T) {
	err := newPanicError("Somewhere", "Something went wrong")
	if err.Error() != "Somewhere panicked with: Something went wrong" {
		t.Fatalf("PanicError should have message \"Somewhere panicked with: Something went wrong\", but got \"%v\"", err.Error())
	}
}
