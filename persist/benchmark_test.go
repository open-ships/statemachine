package persist_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/persist"
)

func benchmarkMemoryMachine() *statemachine.Machine[int, int, struct{}] {
	return statemachine.MustCompile([]statemachine.Transition[int, int, struct{}]{
		{From: 0, Event: 0, To: 1},
		{From: 1, Event: 0, To: 0},
	})
}

// One operation is one successfully committed event.
func BenchmarkMemoryStoreFire(b *testing.B) {
	store := persist.NewMemoryStore(map[string]int{"key": 0})
	m := benchmarkMemoryMachine()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := persist.Fire(context.Background(), store, "key", m, 0, nil); err != nil {
			b.Fatal(err)
		}
	}
}

// One operation is one attempt against a contended key. Report conflicts and
// per-commit cost so failed conditional writes do not inflate throughput.
func BenchmarkMemoryStoreContendedKey(b *testing.B) {
	store := persist.NewMemoryStore(map[string]int{"key": 0})
	m := benchmarkMemoryMachine()
	var commits, conflicts atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := persist.Fire(context.Background(), store, "key", m, 0, nil)
			switch {
			case err == nil:
				commits.Add(1)
			case errors.Is(err, persist.ErrConflict):
				conflicts.Add(1)
			default:
				b.Error(err)
			}
		}
	})
	b.StopTimer()
	snapshot, err := store.Load(context.Background(), "key")
	if err != nil || uint64(snapshot.Revision) != uint64(commits.Load()) {
		b.Fatalf("snapshot = %+v, %v; commits = %d", snapshot, err, commits.Load())
	}
	b.ReportMetric(float64(conflicts.Load())/float64(b.N), "conflicts/op")
	if n := commits.Load(); n != 0 {
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/commit")
	}
}
