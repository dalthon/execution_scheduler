package execution_scheduler

import (
	"reflect"
	"runtime"
	"time"

	"testing"
)

func executionStatusToString(status ExecutionStatus) string {
	switch status {
	case ExecutionPending:
		return "Pending"
	case ExecutionRunning:
		return "Running"
	case ExecutionExpired:
		return "Expired"
	case ExecutionFinished:
		return "Finished"
	default:
		return "Unknown"
	}
}

func TestExecutionCalling(t *testing.T) {
	scheduler := newMockedScheduler()
	handler := newSimpleHandlers(t, 1)
	execution := NewExecution(handler.handler, handler.errorHandler, Parallel, 0)
	goroutineCount := runtime.NumGoroutine()

	if execution.status != ExecutionPending {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.status))
	}

	if !execution.call(scheduler) {
		t.Fatalf("execution.call should run handlers")
	}

	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("execution calling should have spawn only one new goroutine, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.call(scheduler) {
		t.Fatalf("execution.call should not run handler more than once")
	}

	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("execution calling should have spawn only one new goroutine, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.expire(scheduler) {
		t.Fatalf("execution can't be expired after it was called")
	}

	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("execution calling should have spawn only one new goroutine, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.status != ExecutionRunning {
		t.Fatalf("execution status after calling should be Running, but got %q", executionStatusToString(execution.status))
	}

	handler.wait()
	if execution.status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.status))
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if !reflect.DeepEqual(scheduler.events, []ExecutionEvent{FinishedEvent}) {
		t.Fatalf("scheduler should have received only a single FinishedEvent")
	}

	if execution.call(scheduler) {
		t.Fatalf("execution.call should not run handler more than once")
	}

	if execution.status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.status))
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.expire(scheduler) {
		t.Fatalf("execution can't be expired after it was finished")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.status))
	}
}

func TestExecutionExpiration(t *testing.T) {
	scheduler := newMockedScheduler()
	handler := newSimpleHandlers(t, 1)
	execution := NewExecution(handler.handler, handler.errorHandler, Parallel, 0)
	goroutineCount := runtime.NumGoroutine()

	if execution.status != ExecutionPending {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.status))
	}

	execution.setExpiration(scheduler, 3*time.Second)
	if execution.status != ExecutionPending {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.status))
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	scheduler.advance(3001 * time.Millisecond)
	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("only one new goroutine should have being spawned, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	handler.wait()
	if execution.status != ExecutionExpired {
		t.Fatalf("execution status should be expired, but got %q", executionStatusToString(execution.status))
	}

	if !reflect.DeepEqual(scheduler.events, []ExecutionEvent{ErrorEvent}) {
		t.Fatalf("scheduler should have received only a single ErrorEvent")
	}

	if execution.call(scheduler) {
		t.Fatalf("execution can't be called after being expired")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.status != ExecutionExpired {
		t.Fatalf("execution status should be expired, but got %q", executionStatusToString(execution.status))
	}

	if execution.expire(scheduler) {
		t.Fatalf("can't be expired more than once")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.status != ExecutionExpired {
		t.Fatalf("execution status should be expired, but got %q", executionStatusToString(execution.status))
	}
}
