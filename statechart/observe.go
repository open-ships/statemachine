package statechart

import (
	"context"

	"github.com/open-ships/statemachine"
)

// Observation is one committed Statechart node exit or entry. The embedded
// Observation supplies ordering and node membership; Info identifies the
// selected transition, including inherited Handler and final Destination.
type Observation[S, E comparable] struct {
	statemachine.Observation[S, E]
	Info Info[S, E]
}

// Observer receives committed node changes from one Statechart Instance. Data
// is the same shallow value supplied to Fire, observed after entry processing
// terminates.
//
// Delivery is synchronous and ordered by Seq, then observer attachment order,
// but isolated: Fire waits for Observer to return, while a panic or
// runtime.Goexit in Observer is contained and cannot replace an action error or
// panic. Later deliveries are still attempted. One Observer attached to
// several Instances may be called concurrently and must synchronize any shared
// mutable state.
type Observer[S, E comparable, T any] func(context.Context, Observation[S, E], T)

// Observers combines observers in argument order. Nil observers are ignored.
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

func deliverTransitionObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	step uint64,
	exits, entries []S,
	event E,
	info Info[S, E],
	data T,
) {
	nodeCount := len(exits) + len(entries)
	if len(observers) == 0 || nodeCount == 0 {
		return
	}
	total := len(observers) * nodeCount
	for next := 0; next < total; {
		done := make(chan struct{})
		start := next
		go func() {
			defer close(done)
			for index := start; index < total; index++ {
				next = index + 1
				func() {
					defer func() { _ = recover() }()
					nodeIndex := index / len(observers)
					var state S
					move := statemachine.Entered
					if nodeIndex < len(exits) {
						move = statemachine.Exited
						state = exits[nodeIndex]
					} else {
						state = entries[nodeIndex-len(exits)]
					}
					observation := Observation[S, E]{
						Observation: statemachine.Observation[S, E]{
							Seq: step + uint64(nodeIndex), Step: step, Run: step,
							Remaining: uint64(nodeCount - nodeIndex - 1),
							Move:      move, State: state, Event: event,
						},
						Info: info,
					}
					observer := observers[index%len(observers)]
					observer(ctx, observation, data)
				}()
			}
		}()
		<-done
	}
}
