package statemachine

import (
	"context"
	"fmt"
	"time"

	internalobserver "github.com/open-ships/statemachine/internal/observer"
)

// ErrObserverFailed reports a contained Observer panic or runtime.Goexit.
var ErrObserverFailed = internalobserver.ErrFailed

// ErrObserverTimeout reports that a delivery wrapped by [TimeoutObserver]
// exceeded its bound. The observer goroutine may still be running.
var ErrObserverTimeout = internalobserver.ErrTimeout

// ObserverError preserves one contained Observer failure and its original
// stack. The observed state change remains committed.
// It matches ErrObserverFailed and, when Value is an error, preserves that
// error in its unwrap chain, including ErrObserverTimeout and nested failures.
type ObserverError = internalobserver.Error

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
// still attempted. An Observer attached to several executions may be called
// concurrently. TimeoutObserver can also overlap successive deliveries within
// one execution when an earlier callback outlives its timeout. Such observers
// must synchronize shared mutable state.
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
	return internalobserver.Combine(observers, observationSequence[S, E])
}

// TimeoutObserver bounds one observer's synchronous delivery. The wrapped
// observer runs in its own goroutine; if it has not returned after timeout,
// delivery fails with an error matching [ErrObserverTimeout] and
// [ErrObserverFailed] while the committed state change stands. Panics and
// runtime.Goexit inside the wrapped observer keep their original stacks.
//
// The callback receives a context derived from the request with the timeout
// deadline; its values and earlier cancellation are preserved. The context is
// canceled when delivery finishes, so cooperative callbacks can stop promptly.
// Request cancellation does not skip delivery or by itself cause a timeout.
//
// A timed-out goroutine can keep running and overlap later deliveries, even
// within the same execution. Shared mutable state must therefore be protected.
// A callback that ignores cancellation and blocks permanently leaks one
// goroutine per timed-out delivery; the timeout only bounds the caller's wait.
//
// timeout must be positive; TimeoutObserver panics otherwise, because an
// unbounded bound is a construction defect, not a runtime condition. A nil
// observer returns nil, which deliveries ignore.
func TimeoutObserver[S, E comparable, T any](
	observer Observer[S, E, T],
	timeout time.Duration,
) Observer[S, E, T] {
	return internalobserver.Timeout(observer, timeout, observationSequence[S, E])
}

// ContainedObserver separates observation health from the transition error
// channel. Failures of the wrapped observer — panic, runtime.Goexit, or a
// [TimeoutObserver] timeout — never join the error returned by Fire: they are
// passed to report as an error matching [ErrObserverFailed], and the delivery
// is treated as successful.
//
// Use this to keep observation failures separate from execution errors. It
// changes only observation errors: a queued Run can still commit earlier
// events before a later event fails. A nil report drops failures.
// Reporting is synchronous in an isolated goroutine: a panic or
// runtime.Goexit in report is contained and dropped, but a blocked report holds
// delivery indefinitely. Wrap the returned Observer in TimeoutObserver to bound
// reporting as well. A nil observer returns nil, which deliveries ignore.
func ContainedObserver[S, E comparable, T any](
	observer Observer[S, E, T],
	report func(error),
) Observer[S, E, T] {
	return internalobserver.Contained(observer, report, observationSequence[S, E])
}

func copyObservers[S, E comparable, T any](observers []Observer[S, E, T]) []Observer[S, E, T] {
	return internalobserver.Copy(observers)
}

func observationSequence[S, E comparable](observation Observation[S, E]) uint64 {
	return observation.Seq
}

func deliverObservations[S, E comparable, T any](
	observers []Observer[S, E, T],
	ctx context.Context,
	observations []Observation[S, E],
	data T,
) error {
	return internalobserver.Deliver(observers, ctx, len(observations), func(index int) (Observation[S, E], uint64) {
		return observations[index], observations[index].Seq
	}, data)
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
