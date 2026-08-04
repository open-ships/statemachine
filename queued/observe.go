package queued

import (
	"context"

	"github.com/open-ships/statemachine"
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
	observers []statemachine.Observer[S, E, T],
	ctx context.Context,
	step, run uint64,
	from, to S,
	event E,
	data T,
) {
	observations := [...]statemachine.Observation[S, E]{
		{Seq: step, Step: step, Run: run, Remaining: 1, Move: statemachine.Exited, State: from, Event: event},
		{Seq: step + 1, Step: step, Run: run, Move: statemachine.Entered, State: to, Event: event},
	}
	deliverObservations(observers, ctx, observations[:], data)
}

func observationContext(ctx context.Context, active func() bool) context.Context {
	x := &execution{
		active: active,
		enqueue: func(context.Context, any, any) error {
			return ErrNotRunning
		},
	}
	return context.WithValue(ctx, executionKey{}, x)
}
