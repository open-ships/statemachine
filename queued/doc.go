// Package queued owns a machine's current state and runs each call to Fire as
// a serialized, run-to-completion cascade.
//
// A transition effect can append work to its current cascade through its named
// Runtime:
//
//	var run *queued.Runtime[State, Event, *Command]
//	Do: func(ctx context.Context, cmd *Command) error {
//		return run.Enqueue(ctx, notify, cmd)
//	}
//
// Follow-up events run in FIFO order before the next external Fire.
// Runtime.Enqueue is deliberately different from recursively calling
// Runtime.Fire: a recursive
// synchronous call could not complete until the cascade that made it had
// completed. Runtime.Fire detects that mistake when the callback keeps its
// supplied context and reports [ErrReentrant]. Never replace the context to
// evade that check; the call can deadlock behind its own root. Synchronous Fire
// from a callback to another queued Runtime is rejected too, avoiding
// cross-runtime wait cycles.
//
// Runtime publishes a destination only after the selected transition's Do
// returns nil. That makes State the last committed state; it does not make
// arbitrary effects transactional. A Do that returns an error or panics may
// already have changed data or external systems.
//
// Keep state in Runtime. A second authoritative state field in the data passed
// to Fire can disagree with it, especially on errors and panics.
//
// Runtime has finite outstanding-root and cumulative Run-event limits; use
// [NewWithLimits] to configure them, or [NewWithOptions] to combine custom
// limits with observers. A canceled root that has not started is removed and
// returns promptly. LastPanic preserves one bounded originating stack while
// Fire continues to re-panic the original value in its caller's goroutine.
//
// [NewWithObservers] attaches immutable observers to the Runtime. Every
// non-self committed item emits an exit and entry before the next follow-up
// begins. All Steps in one Run share the Run identifier of its first non-empty
// Step; a self-transition root can therefore be silent while a changing
// follow-up establishes that identifier. Observer delivery is synchronous;
// panic and runtime.Goexit failures retain their stacks and return as
// post-commit errors matching statemachine.ErrObserverFailed. Its context
// preserves cancellation and values, rejects Runtime.Fire as reentrant, and
// deliberately refuses Enqueue so observation cannot extend the Run.
package queued
