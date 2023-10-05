package main

import (
	"errors"
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
	scheduler.Schedule(handler(0), errorHandler, s.Critical, 4)
	scheduler.Schedule(handler(1), errorHandler, s.Serial, 1)
	scheduler.Schedule(handler(2), errorHandler, s.Serial, 2)
	scheduler.Schedule(handler(3), errorHandler, s.Serial, 3)
	scheduler.Schedule(handler(4), errorHandler, s.Parallel, 1)
	scheduler.Schedule(handler(5), errorHandler, s.Parallel, 2)
	scheduler.Schedule(handler(6), errorHandler, s.Parallel, 3)

	// Start running scheduler
	scheduler.Run()

	// Wait for scheduler to finish
	waitGroup.Wait()
}

func handler(value int) func() error {
	return func() error {
		time.Sleep(1 * time.Second)
		fmt.Printf("message #%d!\n", value)

		if (value % 2) == 0 {
			return errors.New("Even error!")
		}

		panic("Odd panics!")
	}
}

func errorHandler(err error) error {
	fmt.Printf("recovered error: \"%v\"\n", err)
	time.Sleep(1 * time.Second)
	return nil
}
