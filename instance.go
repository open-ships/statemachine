package statemachine

import (
	"context"
	"errors"
	"iter"
	"sync"
)

// ErrInFlight reports that an event could not be fired because this Instance
// is already firing another event.
//
// Instance rejects overlap rather than waiting. This also makes a recursive
// call to Fire from one of the Instance's own Guards or effects fail instead
// of observing or overwriting a transition that has not committed yet.
var ErrInFlight = errors.New("transition already in flight")

// An Instance owns the current state of one aggregate using an immutable
// Machine as its transition definition.
//
// An Instance is safe for concurrent use. At most one call to Fire may be in
// progress: an overlapping or recursive call returns ErrInFlight immediately.
// State and Permitted continue to report the last committed state while a
// transition is running. No Instance lock is held while a Guard or Do runs.
// Instance synchronizes only its owned state and in-flight marker; the caller
// still owns and synchronizes mutable fields in T.
//
// Do not keep another authoritative copy of the state in T. The Machine reads
// the state owned by the Instance, while a state field in T would be an
// independent value that the Instance neither reads nor updates.
//
// The zero Instance uses the zero Machine and the zero state and is ready for
// use. An Instance must not be copied after first use.
type Instance[S, E comparable, T any] struct {
	mu        sync.Mutex
	machine   Machine[S, E, T]
	state     S
	inFlight  bool
	observers []Observer[S, E, T]
	seq       uint64
}

// NewInstance returns an Instance in initial state. It copies the immutable
// Machine value, so replacing the variable that machine points to later does
// not replace the Instance's definition. A nil machine means the zero Machine,
// which refuses every event.
//
// Construction is the restoration seam: no unrestricted state setter is
// provided. Construct a new Instance with the state loaded for that aggregate.
func NewInstance[S, E comparable, T any](machine *Machine[S, E, T], initial S) *Instance[S, E, T] {
	return newInstance(machine, initial, nil)
}

// NewInstanceWithObservers is like [NewInstance] and attaches observers for
// the lifetime of the returned Instance. Construction and restoration emit no
// observations. Nil observers are ignored.
func NewInstanceWithObservers[S, E comparable, T any](
	machine *Machine[S, E, T],
	initial S,
	observers ...Observer[S, E, T],
) *Instance[S, E, T] {
	return newInstance(machine, initial, observers)
}

func newInstance[S, E comparable, T any](
	machine *Machine[S, E, T],
	initial S,
	observers []Observer[S, E, T],
) *Instance[S, E, T] {
	i := &Instance[S, E, T]{state: initial, observers: copyObservers(observers)}
	if machine != nil {
		i.machine = *machine
	}
	return i
}

// State reports the last committed state.
//
// While Fire is running a Guard or Do, State continues to report the state the
// transition started from. The destination becomes visible only after Do
// returns nil.
func (i *Instance[S, E, T]) State() S {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.state
}

// Fire applies event to the Instance's last committed state.
//
// If the Machine accepts the event and Do succeeds, Fire publishes and returns
// the destination state. A refusal or Do error leaves the state unchanged and
// returns the Machine's error without wrapping it. If a Guard or Do panics,
// Fire clears the in-flight marker, leaves the state unchanged, and propagates
// the panic; effects performed before the panic are not rolled back.
//
// Fire is fail-fast single-flight. If another Fire is already running on this
// Instance, including a recursive Fire from its own Guard or Do, Fire returns
// the current committed state and ErrInFlight without running the Machine.
func (i *Instance[S, E, T]) Fire(ctx context.Context, event E, data T) (S, error) {
	i.mu.Lock()
	if i.inFlight {
		state := i.state
		i.mu.Unlock()
		return state, ErrInFlight
	}
	i.inFlight = true
	from := i.state
	machine := i.machine
	i.mu.Unlock()

	defer func() {
		i.mu.Lock()
		i.inFlight = false
		i.mu.Unlock()
	}()

	next, err := machine.Fire(ctx, from, event, data)

	var step uint64
	var observers []Observer[S, E, T]

	i.mu.Lock()
	if err == nil {
		i.state = next
		if from != next && len(i.observers) != 0 {
			step = i.seq + 1
			i.seq += 2
			observers = i.observers
		}
	}
	committed := i.state
	i.mu.Unlock()

	if len(observers) != 0 {
		deliverTransitionObservations(observers, ctx, step, step, from, next, event, data)
	}

	return committed, err
}

// Permitted returns an eager snapshot of the events accepted in the last
// committed state, paired with their destinations.
//
// Permitted captures the state under synchronization, then runs every relevant
// Guard and copies every result before returning. No Instance lock is held
// while Guards run. Consequently Guards have all run by the time Permitted
// returns, the returned iterator remains valid if the Instance or data changes,
// and stopping iteration early does not leave Guards uncalled. Permitted may
// overlap a Fire callback or another Permitted call; the caller must synchronize
// mutable data in T and keep Guards pure.
func (i *Instance[S, E, T]) Permitted(ctx context.Context, data T) iter.Seq2[E, S] {
	i.mu.Lock()
	from := i.state
	machine := i.machine
	i.mu.Unlock()

	type choice struct {
		event E
		to    S
	}
	var choices []choice
	for event, to := range machine.Permitted(ctx, from, data) {
		choices = append(choices, choice{event: event, to: to})
	}

	return func(yield func(E, S) bool) {
		for _, choice := range choices {
			if !yield(choice.event, choice.to) {
				return
			}
		}
	}
}
