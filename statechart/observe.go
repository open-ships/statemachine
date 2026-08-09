package statechart

import (
	"context"
	"errors"
	"time"

	"github.com/open-ships/statemachine"
	internalobserver "github.com/open-ships/statemachine/internal/observer"
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
// runtime.Goexit in Observer is contained and reported as a post-commit error
// matching statemachine.ErrObserverFailed. Later deliveries are still attempted. One Observer attached to
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
	return &statemachine.ObserverError{
		Observer: index, Seq: observation.Seq, Value: failure.Value,
		Stack: failure.Stack, Stopped: failure.Stopped,
	}
}

func deliverTransitionObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	step uint64,
	exits, entries []S,
	event E,
	info Info[S, E],
	data T,
) error {
	nodeCount := len(exits) + len(entries)
	if len(observers) == 0 || nodeCount == 0 {
		return nil
	}
	at := time.Now()
	var failures []error
	for nodeIndex := 0; nodeIndex < nodeCount; nodeIndex++ {
		for observerIndex, observer := range observers {
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
					At:        at, Move: move, State: state, Event: event,
				},
				Info: info,
			}
			if err := callObserver(observerIndex, observer, ctx, observation, data); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}
