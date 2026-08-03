// Package queued owns a machine's current state and runs each call to Fire as
// a serialized, run-to-completion cascade.
//
// A transition effect can append work to its current cascade with [Enqueue]:
//
//	Do: func(ctx context.Context, cmd *Command) error {
//		return queued.Enqueue(ctx, notify, cmd)
//	}
//
// Follow-up events run in FIFO order before the next external Fire. Enqueue is
// deliberately different from recursively calling Runtime.Fire: a recursive
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
package queued
