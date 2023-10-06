package main

import (
	"fmt"
	"sync"
	"time"

	s "github.com/dalthon/execution_scheduler"
)

type CounterCtx struct {
	count uint
}

func main() {
	var waitGroup sync.WaitGroup

	// Initialize scheduler
	scheduler := s.NewScheduler(&s.Options[*CounterCtx]{}, &CounterCtx{count: 0}, &waitGroup)

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

func handler(value int) func(*CounterCtx) error {
	return func(counter *CounterCtx) error {
		time.Sleep(1 * time.Second)
		fmt.Printf("message #%d! so far we had %d other messages!\n", value, counter.count)
		counter.count++
		return nil
	}
}

func errorHandler(_ *CounterCtx, err error) error {
	return nil
}
