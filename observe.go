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

func (e *ObserverError) Unwrap() error { return ErrObserverFailed }

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
