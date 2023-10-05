package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	s "github.com/dalthon/execution_scheduler"
)

var startedAt time.Time
var scheduler *s.Scheduler

func main() {
	var waitGroup sync.WaitGroup

	// Set all possible options
	options := &s.Options{
		ExecutionTimeout: 4 * time.Second,
		InactivityDelay:  5 * time.Second,
		OnPrepare:        delayedErrorCallback("OnPrepare"),
		OnClosing:        onClosingCallback,
		OnError:          delayedErrorCallback("OnError"),
		OnLeaveError:     delayedErrorCallback("OnLeaveError"),
		OnInactive:       delayedErrorCallback("OnInactive"),
		OnLeaveInactive:  delayedErrorCallback("OnLeaveInactive"),
		OnCrash:          delayedCallback("OnCrash"),
		OnClose:          delayedCallback("OnClose"),
	}

	// Signal handling
	trapSignals()

	// Print a header in console
	fmt.Println("\n[  STATUS  ] @ TIME          | MESSAGE")

	// Initialize scheduler
	scheduler = s.NewScheduler(options, &waitGroup)
	print("started!")

	// Schedules all operations in another goroutine
	go func() {
		print("scheduled first 3 messages in PARALLEL")
		scheduler.Schedule(printMessage("1st message!"), noopErrorHandler, s.Parallel, 0)
		scheduler.Schedule(printMessage("2nd message!"), noopErrorHandler, s.Parallel, 0)
		scheduler.Schedule(printMessage("3rd message!"), noopErrorHandler, s.Parallel, 0)

		time.Sleep(5 * time.Second)
		print("scheduled 4th message, 1st error, and 5th message, but SERIALLY")
		scheduler.Schedule(printMessage("4th message!"), noopErrorHandler, s.Serial, 3)
		scheduler.Schedule(raiseError("1st error!"), errorHandler, s.Serial, 2)
		scheduler.Schedule(printMessage("5th message!"), errorHandlerWithTarget("5th message"), s.Serial, 1)

		time.Sleep(5 * time.Second)
		print("scheduled 6th, 7th and 8th messages, those are CRITICAL, SERIAL and PARALLEL, respectifully")
		scheduler.Schedule(printMessage("6th message!"), noopErrorHandler, s.Critical, 3)
		scheduler.Schedule(printMessage("7th message!"), noopErrorHandler, s.Serial, 2)
		scheduler.Schedule(printMessage("8th message!"), noopErrorHandler, s.Parallel, 1)
	}()

	// Start scheduler
	scheduler.Run()

	// Wait for scheduler to finish
	waitGroup.Wait()

	// Since in this example we don't expect the scheduler to finish with an error
	// unless it is interrupted by a trapped signal, we check `scheduler.Err` for
	// errors.
	//
	// If no error is present there we assume it finished successfully.
	//
	// If some error is there, we assumed `waitGroup.Wait()` waited for proper
	// gracefull shutdown.
	if scheduler.Err == nil {
		print("finished SUCCESSFULLY!\n")
	} else {
		print("finished GRACEFULLY!\n")
	}
}

// Handler generators

func printMessage(message string) func() error {
	return printSlowMessage(1, message)
}

func printSlowMessage(delay int, message string) func() error {
	return func() error {
		time.Sleep(time.Duration(delay) * time.Second)
		print(message)
		return nil
	}
}

func raiseError(message string) func() error {
	return raiseSlowError(1, message)
}

func raiseSlowError(delay int, message string) func() error {
	return func() error {
		time.Sleep(time.Duration(delay) * time.Second)
		print(message)
		return errors.New(message)
	}
}

// Simple handlers

func noopErrorHandler(err error) error {
	message := fmt.Sprintf("Received error %v", err)
	print(message)
	return nil
}

func errorHandlerWithTarget(target string) func(error) error {
	return func(err error) error {
		message := fmt.Sprintf("Received error \"%v\" on \"%s\"", err, target)
		print(message)
		return errors.New(message)
	}
}

func errorHandler(err error) error {
	message := fmt.Sprintf("Received error \"%v\"", err)
	print(message)
	return errors.New(message)
}

// Callbacks

func onClosingCallback(scheduler *s.Scheduler) error {
	print("running onClosing...")
	time.Sleep(1 * time.Second)
	print("finished onClosing with error!")

	return errors.New("closing errored!")
}

func delayedErrorCallback(name string) func(*s.Scheduler) error {
	return func(scheduler *s.Scheduler) error {
		print(fmt.Sprintf("running %s...", name))
		time.Sleep(1 * time.Second)
		print(fmt.Sprintf("finished %s!", name))
		return nil
	}
}

func delayedCallback(name string) func(*s.Scheduler) {
	return func(scheduler *s.Scheduler) {
		print(fmt.Sprintf("running %s...", name))
		time.Sleep(1 * time.Second)
		print(fmt.Sprintf("finished %s!", name))
	}
}

// Signal handling

func trapSignals() {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt)
	go func() {
		received := <-signalChannel
		fmt.Println(" Signal!")
		switch received {
		case syscall.SIGHUP:
			print("received SIGHUP signal")
		case syscall.SIGINT:
			print("received SIGINT signal")
		case syscall.SIGTERM:
			print("received SIGTERM signal")
		case syscall.SIGQUIT:
			print("received SIGQUIT signal")
		default:
			print(fmt.Sprintf("received %v signal", received))
		}
		scheduler.Shutdown()
	}()
}

// Utility functions

func print(message string) {
	var timestamp time.Duration
	if startedAt.IsZero() {
		startedAt = time.Now()
		timestamp = time.Duration(0)
	} else {
		timestamp = time.Since(startedAt)
	}

	fmt.Printf("[ %8s ] @ %-13s | %s\n", schedulerStatus(), timestamp, message)
}

func schedulerStatus() string {
	switch scheduler.Status {
	case s.PendingStatus:
		return "Pending"
	case s.ActiveStatus:
		return "Active"
	case s.InactiveStatus:
		return "Inactive"
	case s.ClosingStatus:
		return "Closing"
	case s.ClosedStatus:
		return "Closed"
	case s.ErrorStatus:
		return "Error"
	case s.CrashedStatus:
		return "Crashed"
	case s.ShutdownStatus:
		return "Shutdown"
	default:
		return "Unknown"
	}
}
