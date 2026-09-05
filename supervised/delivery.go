package supervised

import (
	"context"
	"runtime/debug"
)

// deliveryGate admits at most one adapter callback, without a waiting queue.
// The callback owns its lease until it stops, including after a caller timeout.
// Access to busy is protected by the owning Supervisor's mu.
type deliveryGate struct{ busy bool }

func (s *Supervisor[S, E, T]) callbackReturnedLocked(op *activeOperation[S, E]) {
	if op == nil {
		return
	}
	op.callbacks--
	if s.operation == op && op.returned && op.callbacks == 0 {
		s.operation = nil
	}
}

func (s *Supervisor[S, E, T]) reportingReturned() {
	s.mu.Lock()
	s.reporting--
	s.mu.Unlock()
}

// finalize owns every callback started by an admitted public operation. Private
// test entry points call it too, so tests cross the same protocol as callers.
func (s *Supervisor[S, E, T]) finalize(kind RecordKind, result Result[S, E]) Result[S, E] {
	if result.owner != nil {
		defer s.operationReturned(result.owner)
	}
	s.completeResult(&result)
	s.closeJournalAfter(result)
	s.recordResult(kind, result)
	result.owner = nil
	return result
}

// deliverAdapter never queues behind another delivery. A busy sink is an
// explicit delivery failure; the decision and its in-memory Record stand.
// Adapter ownership and health last until the goroutine actually finishes.
func (s *Supervisor[S, E, T]) deliverAdapter(
	ctx context.Context,
	gate *deliveryGate,
	op *activeOperation[S, E],
	journal bool,
	callback func(context.Context) error,
) error {
	if err := ctx.Err(); err != nil {
		return contextFailure(ctx, err)
	}
	s.mu.Lock()
	if gate.busy {
		s.mu.Unlock()
		return ErrDeliveryBusy
	}
	gate.busy = true
	if op != nil {
		op.callbacks++
	}
	if journal {
		s.journalRunning = true
	} else {
		s.recorderRunning = true
	}
	s.mu.Unlock()
	done := make(chan error, 1)
	go func() {
		var failure error
		returned := false
		defer func() {
			if !returned {
				if value := recover(); value != nil {
					failure = &PanicError{Value: value, Stack: string(debug.Stack())}
				} else {
					failure = ErrExecutionStopped
				}
			}
			s.mu.Lock()
			s.callbackReturnedLocked(op)
			if journal {
				s.journalRunning = false
			} else {
				s.recorderRunning = false
			}
			// Release under mu so admission cannot observe healthy state while
			// the previous callback still owns its gate.
			gate.busy = false
			s.mu.Unlock()
			done <- failure
		}()
		if err := ctx.Err(); err != nil {
			failure = contextFailure(ctx, err)
		} else {
			failure = captureErrorText(callback(ctx))
		}
		returned = true
	}()
	select {
	case failure := <-done:
		return failure
	case <-ctx.Done():
		select {
		case failure := <-done:
			return failure
		default:
			return contextFailure(ctx, ctx.Err())
		}
	}
}
