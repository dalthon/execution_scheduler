# Execution Scheduler

[API Reference][api-reference]

This project allow us to manage control concurrent executions in an easy way.

The easiest way to describe it, is by examples, but you can skip to
[Options](#options) in case you already know how it works and just want to see
quickly how to configure advanced behaviour.

## Examples

Simplest case, also available at [examples/01-simple.go](examples/01-simple.go), call
function `handler` 3 times in parallel.

```go
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
  scheduler.Schedule(handler, errorHandler, s.Parallel, 0)
  scheduler.Schedule(handler, errorHandler, s.Parallel, 0)
  scheduler.Schedule(handler, errorHandler, s.Parallel, 0)

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
```

The same, but with non blocking serial execution, just use `s.Serial` instead of
`s.Parallel` as it is done at [examples/02-serial.go](examples/02-serial.go).

```go
  // Schedule some executions
  scheduler.Schedule(handler, errorHandler, s.Serial, 0)
  scheduler.Schedule(handler, errorHandler, s.Serial, 0)
  scheduler.Schedule(handler, errorHandler, s.Serial, 0)
```

In case you are wondering what is the last argument of `scheduler.Schedule`, it
is priority, see how it is used at [examples/03-priority.go](examples/03-priority.go).

Highest number, highest priority, so bigger runs first.

```go
  // Schedule some executions
  scheduler.Schedule(handler(1), errorHandler, s.Serial, 1)
  scheduler.Schedule(handler(2), errorHandler, s.Serial, 2)
  scheduler.Schedule(handler(3), errorHandler, s.Serial, 3)

  // ...
  func handler(value int) func() error {
    return func() error {
      time.Sleep(1 * time.Second)
      fmt.Printf("message #%d!\n", value)
      return nil
    }
  }
```

With parallel and serial scheduled at the same time, all parallels runs and
serials wait for other serials as you can see at
[examples/04-parallel_and_serials.go](examples/04-parallel_and_serials.go).

```go
  // Schedule some executions
  scheduler.Schedule(handler(1), errorHandler, s.Serial, 1)
  scheduler.Schedule(handler(2), errorHandler, s.Serial, 2)
  scheduler.Schedule(handler(3), errorHandler, s.Serial, 3)
  scheduler.Schedule(handler(4), errorHandler, s.Parallel, 1)
  scheduler.Schedule(handler(5), errorHandler, s.Parallel, 2)
  scheduler.Schedule(handler(6), errorHandler, s.Parallel, 3)

  // outputs:
  // message #6!
  // message #4!
  // message #3!
  // message #5!
  // message #2!
  // message #1!
```

In case you want an execution that runs one after another and don't allow
other parallels at the same time, you can use `Critical` like is done at
[examples/05-critical.go](examples/05-critical.go).

```go
  // Schedule some executions
  scheduler.Schedule(handler(0), errorHandler, s.Critical, 4)
  scheduler.Schedule(handler(1), errorHandler, s.Serial, 1)
  scheduler.Schedule(handler(2), errorHandler, s.Serial, 2)
  scheduler.Schedule(handler(3), errorHandler, s.Serial, 3)
  scheduler.Schedule(handler(4), errorHandler, s.Parallel, 1)
  scheduler.Schedule(handler(5), errorHandler, s.Parallel, 2)
  scheduler.Schedule(handler(6), errorHandler, s.Parallel, 3)

  // outputs:
  // message #0!
  // message #6!
  // message #5!
  // message #4!
  // message #3!
  // message #2!
  // message #1!
```

It may look weird that there is always a second handler named `errorHandler` in
those examples.

It is exactly what you suspected! In case first handler errors or even panics
as you can see at [examples/06-error.go](examples/06-error.go).

```go
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

  // outputs:
  // message #0!
  // recovered error: "Even error!"
  // message #4!
  // message #5!
  // recovered error: "Even error!"
  // recovered error: "Execution panicked with: Odd panics!"
  // message #6!
  // message #3!
  // recovered error: "Even error!"
  // recovered error: "Execution panicked with: Odd panics!"
  // message #2!
  // recovered error: "Even error!"
  // message #1!
  // recovered error: "Execution panicked with: Odd panics!"
```

Going back to initialization of scheduler, you may have noticed that there are
`Options`.

They could be used to define some timeouts and callbacks described at
[options section](#options).

That's it! Now you know everything about this module!

## Options

`Options` struct is given by:

```go
type Options[C any] struct {
  ExecutionTimeout time.Duration
  InactivityDelay  time.Duration
  OnPrepare        func(scheduler *Scheduler[C]) error
  OnClosing        func(scheduler *Scheduler[C]) error
  OnError          func(scheduler *Scheduler[C]) error
  OnLeaveError     func(scheduler *Scheduler[C]) error
  OnInactive       func(scheduler *Scheduler[C]) error
  OnLeaveInactive  func(scheduler *Scheduler[C]) error
  OnCrash          func(scheduler *Scheduler[C])
  OnClose          func(scheduler *Scheduler[C])
}
```

* `ExecutionTimeout`:
Amount of time that an execution can wait before it times out.
If it times out, errorHandler is called with `TimeoutError`.
If it is `time.Duration(0)` (default), it never times out.
* `InactivityDelay`:
Amount of time that scheduler will wait without running executions before
closing.
If it is `time.Duration(0)` (default), it closes imediately once there is no
more executions pending.
* `OnPrepare`:
Callback that will be called during scheduler `Pending` state which is the one
that occurs once scheduler starts to run or once it is recovered from `Error`
state.
If returns an error, it crashes and then closes.
* `OnError`:
Once an execution error handler returns an error it goes to `Error` state.
If more that one parallel execution retuns an error, this callback is called
only once, so it means that it is called only once it enters `Error` state.
* `OnLeaveError`:
Scheduler tries to leave `Error` state when there are no more executions
running.
Then it calls this callback to try to recover from failure.
If it returns an error it goes to `Crashed` state.
Otherwise it goes back to initial `Pending` state.
* `OnInactive`:
Once all executions finish running successfully, scheduler goes from `Active`
state to `Inactive` state.
If it returns an error, it goes to `Crashed` state.
Otherwise it stays on `Inactive` state waiting for new executions to go back to
`Active` state, or goes to `Closing` state if inactivity times out.
* `OnLeaveInactive`:
From `Inactive` state, this callback runs after `OnInactive` and inactivity
times out.
If it returns an error it goes to `Crashed` state.
Otherwise it goes back to initial `Closing` state.
* `OnClosing`:
Runs once we reach `Closing` state.
If it returns an error it goes to `Crashed` state.
Otherwise if there are scheduled executions it goes back to `Active` state.
Otherwise it goes to `Closed` state.
* `OnCrash`:
It runs once scheduler enters `Crashed` state.
* `OnClose`:
It runs once scheduler enters `Closed` state.

So, scheduler has a state machine where all those timeouts and callbacks are
hooked. Those are described in the next section.

## Scheduler State Machine

![Scheduler state machine][state-machine]

There are 8 states. That's right, I know how to count. There are only 7 states
drawn at the diagram because the 8th state is `Shutdown` state that almost all
states can go to once we trigger graceful shutdown with `scheduler.Shutdown`
method.

All states are represented by the given status and descriptions:

* `Pending`:
Preparation state.
Nothing runs there, it waits for `OnPrepare` to setup scheduler.
* `Active`:
Executions runs on this state.
* `Inactive`:
Once no execution is running, it goes to that state.
* `Closing`:
After inactivity times out, it goes to this state.
In this state, every new execution gets scheduled and waits to run once
scheduler goes back to `Active`.
* `Error`:
In this state, every new execution gets scheduled and waits to run once
scheduler if it goes back to `Active` after being recovered.
* `Crashed`:
In this state, something bad enough happened and can't be recovered.
It force closes all pending executions triggering an `TimeoutError`.
It stays in this section until there are no more running or scheduled
executions and then goes to `Closed` state.
* `Shutdown`:
Similar to `Crashed` state, but enters in this once we forcefully shutdown
scheduler.
* `Closed`:
Single and only final state once nothing is running or pending and nothing
new can start.
Once it reaches this state, wait group is released.

## Contributing

Pull requests and issues are welcome! I'll try to review them as soon as I can.

This project is quite simple and its [Makefile][makefile] is quite useful to do
whatever you may need. Run `make help` for more info.

To run tests, run `make test`.

To run test with coverage, run `make cover`.

To run a full featured example available at [examples/main.go][example], run
`make example`.

## License

This project is released under the [MIT License][license]

[api-reference]: https://pkg.go.dev/github.com/dalthon/execution_scheduler
[license]:       https://opensource.org/licenses/MIT
[makefile]:      Makefile
[example]:       examples/main.go
[state-machine]: https://github.com/dalthon/execution_scheduler/blob/master/doc/images/scheduler_statuses.png?raw=true
