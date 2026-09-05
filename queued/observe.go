package queued

import (
	"context"
	"time"

	"github.com/open-ships/statemachine"
	internalobserver "github.com/open-ships/statemachine/internal/observer"
)

func copyObservers[S, E comparable, T any](
	observers []statemachine.Observer[S, E, T],
) []statemachine.Observer[S, E, T] {
	return internalobserver.Copy(observers)
}

func deliverObservations[S, E comparable, T any](
	observers []statemachine.Observer[S, E, T],
	ctx context.Context,
	observations []statemachine.Observation[S, E],
	data T,
) error {
	return internalobserver.Deliver(observers, ctx, len(observations), func(index int) (statemachine.Observation[S, E], uint64) {
		return observations[index], observations[index].Seq
	}, data)
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
