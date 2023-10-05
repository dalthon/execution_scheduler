package main

import (
	"fmt"
	"sync"
	"time"

	s "github.com/dalthon/execution_scheduler"
)

func main() {
	var waitGroup sync.WaitGroup

	// Initialize scheduler
	scheduler := s.NewScheduler(&s.Options[interface{}]{}, nil, &waitGroup)

	// Schedule some executions
	scheduler.Schedule(handler(1), errorHandler, s.Serial, 1)
	scheduler.Schedule(handler(2), errorHandler, s.Serial, 2)
	scheduler.Schedule(handler(3), errorHandler, s.Serial, 3)

	// Start running scheduler
	scheduler.Run()

	// Wait for scheduler to finish
	waitGroup.Wait()
}

func handler(value int) func() error {
	return func() error {
		time.Sleep(1 * time.Second)
		fmt.Printf("message #%d!\n", value)
		return nil
	}
}

func errorHandler(err error) error {
	return nil
}
