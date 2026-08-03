package persist

import (
	"context"
	"fmt"
	"sync"
)

// Revision is an optimistic concurrency token.
type Revision uint64

// Snapshot is one committed state and its revision.
type Snapshot[S comparable] struct {
	State    S
	Revision Revision
}

// MemoryUnit is the unit-of-work value passed to a MemoryStore update.
type MemoryUnit[K comparable] struct {
	Key      K
	Revision Revision
}

// ConflictError describes a failed conditional commit.
type ConflictError struct {
	Expected Revision
	Actual   Revision
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%s: expected revision %d, found %d", ErrConflict, e.Expected, e.Actual)
}

// Unwrap makes errors.Is(err, ErrConflict) true.
func (e *ConflictError) Unwrap() error { return ErrConflict }

// MemoryStore is a versioned, in-memory Store. It deliberately executes the
// callback outside its lock and then uses a conditional commit. Concurrent
// callbacks can therefore both run, but only one can commit; the loser receives
// [ErrConflict] and is not retried. A MemoryStore must not be copied after
// first use.
type MemoryStore[K, S comparable] struct {
	mu      sync.RWMutex
	entries map[K]Snapshot[S]
}

// NewMemoryStore copies initial into a new MemoryStore at revision zero.
func NewMemoryStore[K, S comparable](initial map[K]S) *MemoryStore[K, S] {
	entries := make(map[K]Snapshot[S], len(initial))
	for key, state := range initial {
		entries[key] = Snapshot[S]{State: state}
	}
	return &MemoryStore[K, S]{entries: entries}
}

// Load returns the current committed snapshot for key.
func (s *MemoryStore[K, S]) Load(ctx context.Context, key K) (Snapshot[S], error) {
	if err := ctx.Err(); err != nil {
		return Snapshot[S]{}, err
	}
	s.mu.RLock()
	snapshot, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return Snapshot[S]{}, ErrNotFound
	}
	return snapshot, nil
}

// Update implements [Store]. It checks cancellation before loading and after
// step returns. Cancellation racing the final conditional commit may still
// arrive after the state commits; callers that observe an ambiguous external
// cancellation should reload before deciding whether to retry.
func (s *MemoryStore[K, S]) Update(
	ctx context.Context,
	key K,
	step func(context.Context, S, MemoryUnit[K]) (S, error),
) (S, error) {
	before, err := s.Load(ctx, key)
	if err != nil {
		var zero S
		return zero, err
	}

	next, err := step(ctx, before.State, MemoryUnit[K]{Key: key, Revision: before.Revision})
	if err != nil {
		return before.State, err
	}
	if err := ctx.Err(); err != nil {
		return before.State, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.entries[key] // entries cannot be deleted after construction
	if current.Revision != before.Revision {
		return current.State, &ConflictError{Expected: before.Revision, Actual: current.Revision}
	}
	s.entries[key] = Snapshot[S]{State: next, Revision: before.Revision + 1}
	return next, nil
}
