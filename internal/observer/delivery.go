package observer

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrFailed reports a contained observer panic or runtime.Goexit.
var ErrFailed = errors.New("statemachine: observer failed after commit")

// ErrTimeout reports a delivery whose synchronous wait exceeded its bound.
var ErrTimeout = errors.New("statemachine: observer delivery timed out")

// Error preserves one contained observer failure and its original stack.
// Observer is its zero-based attachment index; Seq identifies the observation.
type Error struct {
	Observer int
	Seq      uint64
	Value    any
	Stack    string
	Stopped  bool
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Stopped {
		return fmt.Sprintf("%v: observer %d stopped at observation %d", ErrFailed, e.Observer, e.Seq)
	}
	return fmt.Sprintf("%v: observer %d panicked at observation %d: %v", ErrFailed, e.Observer, e.Seq, e.Value)
}

// Unwrap preserves both the observer-failure classification and an error-valued
// panic, including timeout errors and failures from nested composition.
func (e *Error) Unwrap() []error {
	if e == nil {
		return nil
	}
	if cause, ok := e.Value.(error); ok {
		return []error{ErrFailed, cause}
	}
	return []error{ErrFailed}
}

func failureError(index int, seq uint64, failure *Failure) error {
	if failure == nil {
		return nil
	}
	return &Error{
		Observer: index, Seq: seq, Value: failure.Value,
		Stack: failure.Stack, Stopped: failure.Stopped,
	}
}

// Copy freezes attachment order and removes nil callbacks without changing the
// caller's observer slice or erasing its observation and data types.
func Copy[O, T any, F ~func(context.Context, O, T)](observers []F) []F {
	result := make([]F, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			result = append(result, observer)
		}
	}
	return result
}

// Deliver publishes count observations in index order, then attachment order.
// at supplies the typed observation and sequence without requiring a temporary
// batch allocation. Each callback is isolated and all failures are joined.
func Deliver[O, T any, F ~func(context.Context, O, T)](
	observers []F,
	ctx context.Context,
	count int,
	at func(int) (O, uint64),
	data T,
) error {
	if len(observers) == 0 || count == 0 {
		return nil
	}
	var failures []error
	for index := 0; index < count; index++ {
		observation, seq := at(index)
		for observerIndex, observer := range observers {
			failure := Call(func() { observer(ctx, observation, data) })
			if err := failureError(observerIndex, seq, failure); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}

// Combine freezes observers and transports any joined failures through a panic
// for the enclosing delivery to contain.
func Combine[O, T any, F ~func(context.Context, O, T)](observers []F, sequence func(O) uint64) F {
	observers = Copy(observers)
	if len(observers) == 0 {
		return nil
	}
	return func(ctx context.Context, observation O, data T) {
		err := Deliver(observers, ctx, 1, func(int) (O, uint64) {
			return observation, sequence(observation)
		}, data)
		if err != nil {
			panic(err)
		}
	}
}

// Timeout bounds the synchronous wait and passes the same deadline to the
// callback. Parent cancellation remains visible but does not skip delivery or
// by itself classify the callback as a timeout.
func Timeout[O, T any, F ~func(context.Context, O, T)](observer F, timeout time.Duration, sequence func(O) uint64) F {
	if timeout <= 0 {
		panic("statemachine: TimeoutObserver requires a positive timeout")
	}
	if observer == nil {
		return nil
	}
	return func(ctx context.Context, observation O, data T) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		done := Start(func() { observer(ctx, observation, data) })
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case failure := <-done:
			if err := failureError(0, sequence(observation), failure); err != nil {
				panic(err)
			}
		case <-timer.C:
			panic(fmt.Errorf("%w after %v", ErrTimeout, timeout))
		}
	}
}

// Contained routes observer failures to a synchronous isolated reporter. The
// report is awaited without a timeout; its own failures are dropped.
func Contained[O, T any, F ~func(context.Context, O, T)](observer F, report func(error), sequence func(O) uint64) F {
	if observer == nil {
		return nil
	}
	return func(ctx context.Context, observation O, data T) {
		failure := Call(func() { observer(ctx, observation, data) })
		err := failureError(0, sequence(observation), failure)
		if err != nil && report != nil {
			_ = Call(func() { report(err) })
		}
	}
}
