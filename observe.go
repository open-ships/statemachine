package statemachine

import (
	"context"
	"errors"
	"fmt"
	"time"

	internalobserver "github.com/open-ships/statemachine/internal/observer"
)

// ErrObserverFailed reports a contained Observer panic or runtime.Goexit.
var ErrObserverFailed = errors.New("statemachine: observer failed after commit")

// ErrObserverTimeout reports that a delivery wrapped by [TimeoutObserver]
// exceeded its bound. The observer goroutine may still be running.
var ErrObserverTimeout = errors.New("statemachine: observer delivery timed out")

// ObserverError preserves one contained Observer failure and its original
// stack. The observed state change remains committed.
type ObserverError struct {
	Observer int
	Seq      uint64
	Value    any
	Stack    string
	Stopped  bool
}

func (e *ObserverError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Stopped {
		return fmt.Sprintf("%v: observer %d stopped at observation %d", ErrObserverFailed, e.Observer, e.Seq)
	}
	return fmt.Sprintf("%v: observer %d panicked at observation %d: %v", ErrObserverFailed, e.Observer, e.Seq, e.Value)
}

// Unwrap makes every ObserverError match ErrObserverFailed and, when the
// contained panic value is itself an error — as it is for [TimeoutObserver]
// timeouts and nested ObserverErrors — that error too, so errors.Is can reach
// [ErrObserverTimeout] through the containment layers.
func (e *ObserverError) Unwrap() []error {
	if e == nil {
		return nil
	}
	if cause, ok := e.Value.(error); ok {
		return []error{ErrObserverFailed, cause}
	}
	return []error{ErrObserverFailed}
}

// Move identifies one committed change in node membership.
type Move uint8

const (
	// Exited reports that State is no longer active.
	Exited Move = iota
	// Entered reports that State became active.
	Entered
)

func (m Move) String() string {
	switch m {
	case Exited:
		return "exited"
	case Entered:
		return "entered"
	default:
		return fmt.Sprintf("Move(%d)", uint8(m))
	}
}

// Observation describes one committed node exit or entry.
//
// Seq is consecutive within one observed execution, starting at one. Step is
// the Seq of the first Observation produced by the same position change. Run is
// the Step of the first observed position change in a queued run; executions
// without cascades set Run equal to Step. Remaining is the number of later
// Observations in this Step, so zero closes a complete batch. At is the wall
// time at which the committed Step was published; every Observation in one
// Step has the same At value.
//
// At is wall-clock time and is subject to clock steps (NTP or GNSS
// discipline): At ordering across a step is not trustworthy, and Seq is the
// authoritative order within one execution. Seq restarts at one for each
// reconstructed execution and carries no execution identity; a census across
// restarts must seed and correlate independently.
//
// Observation deliberately contains neither context.Context nor T. Both are
// supplied to Observer without type erasure. An Observation is comparable when
// S and E are comparable.
type Observation[S, E comparable] struct {
	Seq       uint64
	Step      uint64
	Run       uint64
	Remaining uint64
	At        time.Time
	Move      Move
	State     S
	Event     E
}

// Observer receives committed position changes from one Instance or Runtime.
// The ctx carries the request values and cancellation associated with Fire;
// data is the same shallow value supplied to Fire, not a historical copy.
//
// Delivery is synchronous and ordered by Seq, then observer attachment order,
// but isolated: Fire waits for Observer to return, while a panic or
// runtime.Goexit in Observer is contained, retains its stack, and is returned
// as an error matching ErrObserverFailed after commit. Later deliveries are
// still attempted. One
// Observer attached to several executions may be called concurrently and must
// synchronize any shared mutable state.
type Observer[S, E comparable, T any] func(context.Context, Observation[S, E], T)

// Observers combines observers in argument order. Nil observers are ignored.
// Each observer is isolated, so a panic or runtime.Goexit in one does not stop
// later observers.
//
// The combined Observer reports the joined inner failures by panicking, which
// the delivery boundary of this module contains and returns as an error
// matching [ErrObserverFailed]. Application code that invokes the combined
// Observer directly, outside an Instance or Runtime, must be prepared to
// recover that panic itself.
func Observers[S, E comparable, T any](observers ...Observer[S, E, T]) Observer[S, E, T] {
	observers = copyObservers(observers)
	if len(observers) == 0 {
		return nil
	}
	return func(ctx context.Context, observation Observation[S, E], data T) {
		var failures []error
		for index, observer := range observers {
			if err := callObserver(index, observer, ctx, observation, data); err != nil {
				failures = append(failures, err)
			}
		}
		if len(failures) != 0 {
			panic(errors.Join(failures...))
		}
	}
}

// TimeoutObserver bounds one observer's synchronous delivery. The wrapped
// observer runs in its own goroutine; if it has not returned after timeout,
// delivery fails with an error matching [ErrObserverTimeout] and
// [ErrObserverFailed] while the committed state change stands. Panics and
// runtime.Goexit inside the wrapped observer keep their original stacks.
//
// A timed-out observer goroutine is abandoned, not terminated: it may still be
// running, and an observer that blocks permanently leaks one goroutine per
// timed-out delivery. The bound therefore protects the execution's liveness,
// not the process's total resources; an observer that can block — a network
// logger, a slow sink — should also carry its own internal deadline.
//
// timeout must be positive; TimeoutObserver panics otherwise, because an
// unbounded bound is a construction defect, not a runtime condition. A nil
// observer returns nil, which deliveries ignore.
func TimeoutObserver[S, E comparable, T any](
	observer Observer[S, E, T],
	timeout time.Duration,
) Observer[S, E, T] {
	if timeout <= 0 {
		panic("statemachine: TimeoutObserver requires a positive timeout")
	}
	if observer == nil {
		return nil
	}
	return func(ctx context.Context, observation Observation[S, E], data T) {
		done := internalobserver.Start(func() { observer(ctx, observation, data) })
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case failure := <-done:
			if failure != nil {
				panic(&ObserverError{
					Seq: observation.Seq, Value: failure.Value,
					Stack: failure.Stack, Stopped: failure.Stopped,
				})
			}
		case <-timer.C:
			panic(fmt.Errorf("%w after %v", ErrObserverTimeout, timeout))
		}
	}
}

// ContainedObserver separates observation health from the transition error
// channel. Failures of the wrapped observer — panic, runtime.Goexit, or a
// [TimeoutObserver] timeout — never join the error returned by Fire: they are
// passed to report as an error matching [ErrObserverFailed], on the delivering
// goroutine, and the delivery is treated as successful.
//
// This is the seam for callers that must preserve the invariant that a non-nil
// Fire error implies the transition did not commit. A nil report drops
// failures. A panic or runtime.Goexit in report itself is contained and
// dropped: the reporting seam must never become a new failure channel. A nil
// observer returns nil, which deliveries ignore.
func ContainedObserver[S, E comparable, T any](
	observer Observer[S, E, T],
	report func(error),
) Observer[S, E, T] {
	if observer == nil {
		return nil
	}
	return func(ctx context.Context, observation Observation[S, E], data T) {
		err := callObserver(0, observer, ctx, observation, data)
		if err == nil || report == nil {
			return
		}
		_ = internalobserver.Call(func() { report(err) })
	}
}

func copyObservers[S, E comparable, T any](observers []Observer[S, E, T]) []Observer[S, E, T] {
	result := make([]Observer[S, E, T], 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			result = append(result, observer)
		}
	}
	return result
}

func callObserver[S, E comparable, T any](
	index int,
	observer Observer[S, E, T],
	ctx context.Context,
	observation Observation[S, E],
	data T,
) error {
	failure := internalobserver.Call(func() { observer(ctx, observation, data) })
	if failure == nil {
		return nil
	}
	return &ObserverError{
		Observer: index, Seq: observation.Seq, Value: failure.Value,
		Stack: failure.Stack, Stopped: failure.Stopped,
	}
}

func deliverObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	observations []Observation[S, E],
	data T,
) error {
	if len(observers) == 0 || len(observations) == 0 {
		return nil
	}
	var failures []error
	for _, observation := range observations {
		for index, observer := range observers {
			if err := callObserver(index, observer, ctx, observation, data); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

func deliverTransitionObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	step, run uint64,
	from, to S,
	event E,
	data T,
) error {
	at := time.Now()
	observations := [...]Observation[S, E]{
		{Seq: step, Step: step, Run: run, Remaining: 1, At: at, Move: Exited, State: from, Event: event},
		{Seq: step + 1, Step: step, Run: run, At: at, Move: Entered, State: to, Event: event},
	}
	return deliverObservations(observers, ctx, observations[:], data)
}
