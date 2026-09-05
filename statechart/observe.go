package statechart

import (
	"context"
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
// matching statemachine.ErrObserverFailed. Later deliveries are still attempted.
// An Observer attached to several Instances may be called concurrently.
// TimeoutObserver can also overlap successive deliveries within one Instance
// when an earlier callback outlives its timeout. Such observers must synchronize
// shared mutable state.
type Observer[S, E comparable, T any] func(context.Context, Observation[S, E], T)

// Observers combines observers in argument order. Nil observers are ignored.
// Each observer is isolated, so a panic or runtime.Goexit in one does not stop
// later observers. The combined Observer transports joined failures by panicking;
// Instance.Fire contains that panic as an error matching statemachine.ErrObserverFailed.
// Direct callers must recover it themselves.
func Observers[S, E comparable, T any](observers ...Observer[S, E, T]) Observer[S, E, T] {
	return internalobserver.Combine(observers, observationSequence[S, E])
}

// TimeoutObserver bounds one observer's synchronous delivery while preserving
// the full typed Observation, including its transition Info. It behaves like
// statemachine.TimeoutObserver: a timeout fails with an error matching both
// statemachine.ErrObserverTimeout and statemachine.ErrObserverFailed at the
// Instance's delivery seam. Panics and runtime.Goexit retain their original stacks.
//
// The callback receives a context derived from the request with the timeout
// deadline; its values and earlier cancellation are preserved. The context is
// canceled when delivery finishes. Request cancellation does not skip delivery
// or by itself cause a timeout.
//
// A timed-out goroutine can keep running and overlap later deliveries, even in
// the same Instance. Protect shared mutable state and honor context cancellation:
// a permanently blocked callback leaks one goroutine per timed-out delivery.
// timeout must be positive or TimeoutObserver panics. A nil observer returns nil.
func TimeoutObserver[S, E comparable, T any](observer Observer[S, E, T], timeout time.Duration) Observer[S, E, T] {
	return internalobserver.Timeout(observer, timeout, observationSequence[S, E])
}

// ContainedObserver reports observer failures separately from Fire's return
// value while preserving the typed Observation and Info. A wrapped panic,
// runtime.Goexit, or TimeoutObserver timeout is passed to report as an error
// matching statemachine.ErrObserverFailed and delivery is treated as successful.
//
// A nil report drops failures. Reporting is synchronous in an isolated goroutine:
// report panics and runtime.Goexit are contained and dropped, but a blocked
// report holds delivery indefinitely. Wrap the returned Observer in
// TimeoutObserver to bound reporting as well. A nil observer returns nil.
func ContainedObserver[S, E comparable, T any](observer Observer[S, E, T], report func(error)) Observer[S, E, T] {
	return internalobserver.Contained(observer, report, observationSequence[S, E])
}

func copyObservers[S, E comparable, T any](observers []Observer[S, E, T]) []Observer[S, E, T] {
	return internalobserver.Copy(observers)
}

func observationSequence[S, E comparable](observation Observation[S, E]) uint64 {
	return observation.Seq
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
	return internalobserver.Deliver(observers, ctx, nodeCount, func(nodeIndex int) (Observation[S, E], uint64) {
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
		return observation, observation.Seq
	}, data)
}
