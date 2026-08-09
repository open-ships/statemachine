package queued

import (
	"context"
	"errors"
	"time"

	"github.com/open-ships/statemachine"
	internalobserver "github.com/open-ships/statemachine/internal/observer"
)

func copyObservers[S, E comparable, T any](
	observers []statemachine.Observer[S, E, T],
) []statemachine.Observer[S, E, T] {
	result := make([]statemachine.Observer[S, E, T], 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			result = append(result, observer)
		}
	}
	return result
}

func deliverObservations[S, E comparable, T any](
	observers []statemachine.Observer[S, E, T],
	ctx context.Context,
	observations []statemachine.Observation[S, E],
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

func callObserver[S, E comparable, T any](
	index int,
	observer statemachine.Observer[S, E, T],
	ctx context.Context,
	observation statemachine.Observation[S, E],
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
	observers []statemachine.Observer[S, E, T],
	ctx context.Context,
	step, run uint64,
	from, to S,
	event E,
	data T,
) error {
	at := time.Now()
	observations := [...]statemachine.Observation[S, E]{
		{Seq: step, Step: step, Run: run, Remaining: 1, At: at, Move: statemachine.Exited, State: from, Event: event},
		{Seq: step + 1, Step: step, Run: run, At: at, Move: statemachine.Entered, State: to, Event: event},
	}
	return deliverObservations(observers, ctx, observations[:], data)
}

func observationContext(ctx context.Context, owner any, active func() bool) context.Context {
	x := &execution{
		owner:  owner,
		active: active,
		enqueue: func(context.Context, any, any) error {
			return ErrNotRunning
		},
	}
	return context.WithValue(ctx, executionKey{}, x)
}
