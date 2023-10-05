package execution_scheduler

import (
	"errors"
	"reflect"
	"runtime"
	"time"

	"testing"
)

func TestExecutionCalling(t *testing.T) {
	scheduler := newMockedScheduler()
	handler := newSimpleHandlers(t, 1)
	execution := newExecution(handler.handler, handler.errorHandler, Parallel, 0)
	goroutineCount := runtime.NumGoroutine()

	if execution.Status != ExecutionScheduled {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.Status))
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

	if execution.expire(scheduler, errors.New("Timeout error")) {
		t.Fatalf("execution can't be expired after it was called")
	}

	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("execution calling should have spawn only one new goroutine, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.Status != ExecutionRunning {
		t.Fatalf("execution status after calling should be Running, but got %q", executionStatusToString(execution.Status))
	}

	handler.wait()
	if execution.Status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.Status))
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if !reflect.DeepEqual(scheduler.events, []ExecutionEvent{FinishedParallelEvent}) {
		t.Fatalf("scheduler should have received only a single FinishedParallelEvent")
	}

	if execution.call(scheduler) {
		t.Fatalf("execution.call should not run handler more than once")
	}

	if execution.expire(scheduler, errors.New("Timeout error")) {
		t.Fatalf("execution can't be expired after it was finished")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.Status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.Status))
	}
}

func TestExecutionExpiration(t *testing.T) {
	scheduler := newMockedScheduler()
	handler := newSimpleHandlers(t, 1)
	execution := newExecution(handler.handler, handler.errorHandler, Parallel, 0)
	goroutineCount := runtime.NumGoroutine()

	if execution.Status != ExecutionScheduled {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.Status))
	}

	execution.setExpiration(scheduler, 3*time.Second)
	if execution.Status != ExecutionScheduled {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.Status))
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	scheduler.advance(3 * time.Second)
	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("only one new goroutine should have being spawned, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	handler.wait()
	if execution.Status != ExecutionExpired {
		t.Fatalf("execution status should be expired, but got %q", executionStatusToString(execution.Status))
	}

	if !reflect.DeepEqual(scheduler.events, []ExecutionEvent{ErrorParallelEvent}) {
		t.Fatalf("scheduler should have received only a single ErrorParallelEvent but got %v", scheduler.events)
	}

	if execution.call(scheduler) {
		t.Fatalf("execution can't be called after being expired")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.Status != ExecutionExpired {
		t.Fatalf("execution status should be expired, but got %q", executionStatusToString(execution.Status))
	}

	if execution.expire(scheduler, errors.New("Timeout error")) {
		t.Fatalf("can't be expired more than once")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.Status != ExecutionExpired {
		t.Fatalf("execution status should be expired, but got %q", executionStatusToString(execution.Status))
	}
}

func TestExecutionCallingBeforeExpire(t *testing.T) {
	scheduler := newMockedScheduler()
	handler := newSimpleHandlers(t, 1)
	execution := newExecution(handler.handler, handler.errorHandler, Parallel, 0)
	goroutineCount := runtime.NumGoroutine()

	if execution.Status != ExecutionScheduled {
		t.Fatalf("execution initial status should be Pending, but got %q", executionStatusToString(execution.Status))
	}

	execution.setExpiration(scheduler, 3*time.Second)
	if !execution.call(scheduler) {
		t.Fatalf("execution.call should run handlers")
	}

	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("execution calling should have spawn only one new goroutine, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.call(scheduler) {
		t.Fatalf("execution.call should not run handler more than once")
	}

	if execution.expire(scheduler, errors.New("Timeout error")) {
		t.Fatalf("execution can't be expired after it was called")
	}

	if runtime.NumGoroutine() != goroutineCount+1 {
		t.Fatalf("execution calling should have spawn only one new goroutine, but got %d", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.Status != ExecutionRunning {
		t.Fatalf("execution status after calling should be Running, but got %q", executionStatusToString(execution.Status))
	}

	handler.wait()
	if execution.Status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.Status))
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if !reflect.DeepEqual(scheduler.events, []ExecutionEvent{FinishedParallelEvent}) {
		t.Fatalf("scheduler should have received only a single FinishedParallelEvent")
	}

	if execution.call(scheduler) {
		t.Fatalf("execution.call should not run handler more than once")
	}

	if execution.expire(scheduler, errors.New("Timeout error")) {
		t.Fatalf("execution can't be expired after it was finished")
	}

	if runtime.NumGoroutine() != goroutineCount {
		t.Fatalf("no new goroutines should be spawned, but %d we have", runtime.NumGoroutine()-goroutineCount)
	}

	if execution.Status != ExecutionFinished {
		t.Fatalf("execution status after calling should be Finished, but got %q", executionStatusToString(execution.Status))
	}
}
