package main

import (
	"fmt"
	"sync"
	"time"

	s "github.com/dalthon/execution_scheduler"
)

var count int = 0

func main() {
	var waitGroup sync.WaitGroup

	// Initialize scheduler
	scheduler := s.NewScheduler(&s.Options[interface{}]{}, nil, &waitGroup)

	// Schedule some executions
	scheduler.Schedule(handler, errorHandler, s.Serial, 0)
	scheduler.Schedule(handler, errorHandler, s.Serial, 0)
	scheduler.Schedule(handler, errorHandler, s.Serial, 0)

	// Start running scheduler
	scheduler.Run()

	// Wait for scheduler to finish
	waitGroup.Wait()
}

func handler() error {
	time.Sleep(1 * time.Second)
	count++
	fmt.Printf("message #%d!\n", count)
	return nil
}

func errorHandler(err error) error {
	return nil
}
