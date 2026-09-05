package persist

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/internal/keycheck"
)

func isNilStore(store any) bool {
	if store == nil {
		return true
	}
	value := reflect.ValueOf(store)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var (
	// ErrConflict reports that another writer changed the aggregate after this
	// update loaded it. The transition callback has already run and is never
	// retried by this package.
	ErrConflict = errors.New("persist: version conflict")

	// ErrNotFound reports that a Store has no state for the requested key.
	ErrNotFound = errors.New("persist: state not found")

	// ErrNoStore reports a nil Store or an unconfigured FuncStore.
	ErrNoStore = errors.New("persist: store not configured")

	// ErrInvalidKey reports a Store key that cannot be compared safely or does
	// not equal itself, such as a NaN-containing key.
	ErrInvalidKey = errors.New("persist: key must be comparable and equal itself")
	// ErrStoreContract reports a detectable violation of the Store contract.
	ErrStoreContract = errors.New("persist: store violated the update contract")
	// ErrStepRepeated reports a second callback invocation by one Store.Update.
	ErrStepRepeated = errors.New("persist: store called the transition step more than once")
	// ErrStepNotCalled reports Store.Update success without a callback invocation.
	ErrStepNotCalled = errors.New("persist: store returned success without calling the transition step")
	// ErrStepFailed reports Store success after its transition callback failed.
	ErrStepFailed = errors.New("persist: store returned success after the transition step failed")
	// ErrStepIncomplete reports that a Store returned after its callback stopped
	// without returning, for example by swallowing a panic or runtime.Goexit.
	ErrStepIncomplete = errors.New("persist: store returned after the transition step stopped without returning")
	// ErrStateMismatch reports Store success with a state different from the
	// successful transition callback's destination, or an invalid state value.
	ErrStateMismatch = errors.New("persist: store returned a different committed state")
)

// Store owns one versioned update for an aggregate identified by K. X is the
// adapter's unit-of-work value, such as *sql.Tx.
//
// After loading the state, Update must call step exactly once and commit the
// state step returns only when step succeeds. It must not retry step.
// Fire and Step detect repeated callbacks and successful returns without a
// callback, after a failed callback, or with a different state, and callbacks
// that stopped without returning while the Store still returned. They report
// [ErrStoreContract] and never execute a repeated step. These checks cannot
// verify that an adapter actually persisted its reported result.
// Detection covers calls made before Update returns, including calls from
// other goroutines that Update joins. A Store that violates the contract by
// invoking step again after Update has already returned races with the
// caller's use of the result; no detection window can cover that, so such a
// Store is simply broken.
// Implementations that can race with another process must use a conditional
// write and report an error matching [ErrConflict] when it loses. If step
// panics, the Store must abort its unit of work and propagate the panic.
//
// On success Update returns the committed state. Its state result is not
// authoritative on error: a caller deciding to retry must reload.
type Store[K, S comparable, X any] interface {
	Update(context.Context, K, func(context.Context, S, X) (S, error)) (S, error)
}

// FuncStore adapts an application function to [Store]. It is the intended seam
// for SQL transactions, ORM units of work, and other external persistence.
type FuncStore[K, S comparable, X any] struct {
	UpdateFunc func(context.Context, K, func(context.Context, S, X) (S, error)) (S, error)
}

// StepResult describes the transition attempt made inside a Store update.
// From and To are meaningful only when Attempted is true. TransitionError is
// the exact error returned by the state-owning transition execution, before
// any later Store error.
// Confirmed is true only when Store.Update returned success and detectable
// contract checks passed; false does not prove that an external commit did
// not happen.
type StepResult[S, E comparable] struct {
	From            S
	To              S
	Event           E
	Attempted       bool
	Confirmed       bool
	TransitionError error
}

// Update delegates to UpdateFunc.
func (s FuncStore[K, S, X]) Update(
	ctx context.Context,
	key K,
	step func(context.Context, S, X) (S, error),
) (S, error) {
	if s.UpdateFunc == nil {
		var zero S
		return zero, ErrNoStore
	}
	return s.UpdateFunc(ctx, key, step)
}

// Fire applies event through store's unit of work. data constructs the value
// passed to the Machine from the adapter's X, so an effect can use the same
// transaction that will persist the returned state.
//
// Fire never retries. If machine is nil it behaves like the zero Machine. A nil
// data function supplies the zero value of T.
func Fire[K, S, E comparable, T, X any](
	ctx context.Context,
	store Store[K, S, X],
	key K,
	machine *statemachine.Machine[S, E, T],
	event E,
	data func(X) T,
) (S, error) {
	state, _, err := apply(ctx, store, key, machine, event, data)
	return state, err
}

// Step is like [Fire] but also reports the transition attempt performed inside
// the Store. It is useful for diagnostics and for adapters that need From, which
// is loaded inside Store.Update. StepResult is not a durable observation: write
// durable events through the Store's transactional unit of work instead.
func Step[K, S, E comparable, T, X any](
	ctx context.Context,
	store Store[K, S, X],
	key K,
	machine *statemachine.Machine[S, E, T],
	event E,
	data func(X) T,
) (StepResult[S, E], error) {
	_, result, err := apply(ctx, store, key, machine, event, data)
	return result, err
}

func apply[K, S, E comparable, T, X any](
	ctx context.Context,
	store Store[K, S, X],
	key K,
	machine *statemachine.Machine[S, E, T],
	event E,
	data func(X) T,
) (S, StepResult[S, E], error) {
	result := StepResult[S, E]{Event: event}
	if !keycheck.Value(key) {
		var zero S
		return zero, result, ErrInvalidKey
	}
	if isNilStore(store) {
		var zero S
		return zero, result, ErrNoStore
	}
	if machine == nil {
		machine = new(statemachine.Machine[S, E, T])
	}
	var started atomic.Bool
	var repeated atomic.Bool
	var completed atomic.Bool
	firstDone := make(chan struct{})
	state, err := store.Update(ctx, key, func(ctx context.Context, from S, unit X) (S, error) {
		if !started.CompareAndSwap(false, true) {
			repeated.Store(true)
			return from, errors.Join(ErrStoreContract, ErrStepRepeated)
		}
		defer close(firstDone)
		var value T
		if data != nil {
			value = data(unit)
		}
		result.From = from
		result.To = from
		result.Attempted = true
		execution := statemachine.NewInstance(machine, from)
		to, transitionErr := execution.Fire(ctx, event, value)
		result.To = to
		result.TransitionError = transitionErr
		completed.Store(true)
		return to, transitionErr
	})
	if started.Load() {
		<-firstDone
		if !completed.Load() {
			failure := errors.Join(ErrStoreContract, ErrStepIncomplete, err)
			if repeated.Load() {
				failure = errors.Join(failure, ErrStepRepeated)
			}
			return state, result, failure
		}
	}
	if repeated.Load() {
		result.Confirmed = false
		return state, result, errors.Join(ErrStoreContract, ErrStepRepeated, err)
	}
	if !started.Load() && err == nil {
		return state, result, errors.Join(ErrStoreContract, ErrStepNotCalled)
	}
	if err == nil && started.Load() {
		if result.TransitionError != nil {
			return state, result, errors.Join(ErrStoreContract, ErrStepFailed, result.TransitionError)
		}
		if !keycheck.Value(state) || state != result.To {
			return state, result, errors.Join(ErrStoreContract, ErrStateMismatch)
		}
		result.Confirmed = true
	}
	return state, result, err
}
