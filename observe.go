package statemachine

import (
	"context"
	"fmt"
)

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
// Observations in this Step, so zero closes a complete batch.
//
// Observation deliberately contains neither context.Context nor T. Both are
// supplied to Observer without type erasure. An Observation is comparable when
// S and E are comparable.
type Observation[S, E comparable] struct {
	Seq       uint64
	Step      uint64
	Run       uint64
	Remaining uint64
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
// runtime.Goexit in Observer is contained and cannot change the transition or
// queued Run outcome. Later deliveries are still attempted. Observer must
// arrange its own reporting if it needs to surface such a failure. One
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
		for _, observer := range observers {
			callObserver(observer, ctx, observation, data)
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
	observer Observer[S, E, T],
	ctx context.Context,
	observation Observation[S, E],
	data T,
) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		observer(ctx, observation, data)
	}()
	<-done
}

func deliverObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	observations []Observation[S, E],
	data T,
) {
	if len(observers) == 0 || len(observations) == 0 {
		return
	}
	total := len(observers) * len(observations)
	for next := 0; next < total; {
		done := make(chan struct{})
		start := next
		go func() {
			defer close(done)
			for index := start; index < total; index++ {
				next = index + 1
				func() {
					defer func() { _ = recover() }()
					observation := observations[index/len(observers)]
					observer := observers[index%len(observers)]
					observer(ctx, observation, data)
				}()
			}
		}()
		<-done
	}
}

func deliverTransitionObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	step, run uint64,
	from, to S,
	event E,
	data T,
) {
	observations := [...]Observation[S, E]{
		{Seq: step, Step: step, Run: run, Remaining: 1, Move: Exited, State: from, Event: event},
		{Seq: step + 1, Step: step, Run: run, Move: Entered, State: to, Event: event},
	}
	deliverObservations(observers, ctx, observations[:], data)
}
