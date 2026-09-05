package queued_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

// One operation is one complete Run; events/op and ns/event expose the work
// when comparing different cascade lengths or observer counts.
func BenchmarkRuntimeCascade(b *testing.B) {
	type work struct{ remaining int }
	for _, events := range []int{1, 16, 256} {
		for _, observerCount := range []int{0, 1, 8} {
			b.Run(fmt.Sprintf("events=%d/observers=%d", events, observerCount), func(b *testing.B) {
				var r *queued.Runtime[int, int, *work]
				do := func(ctx context.Context, data *work) error {
					if data.remaining == 0 {
						return nil
					}
					data.remaining--
					return r.Enqueue(ctx, 0, data)
				}
				m := statemachine.MustCompile([]statemachine.Transition[int, int, *work]{
					{From: 0, Event: 0, To: 1, Do: do},
					{From: 1, Event: 0, To: 0, Do: do},
				})
				observers := make([]statemachine.Observer[int, int, *work], observerCount)
				for i := range observers {
					observers[i] = func(context.Context, statemachine.Observation[int, int], *work) {}
				}
				var err error
				r, err = queued.NewWithOptions(m, 0, queued.Options[int, int, *work]{
					Limits: queued.Limits{MaxRoots: 1, MaxRunEvents: events}, Observers: observers,
				})
				if err != nil {
					b.Fatal(err)
				}
				data := &work{}
				b.ReportAllocs()
				for b.Loop() {
					data.remaining = events - 1
					if _, err := r.Fire(context.Background(), 0, data); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(events), "events/op")
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(events), "ns/event")
			})
		}
	}
}

// One operation is an admission attempt. Rejections are reported separately
// so a smaller queue cannot look faster merely by refusing more work.
func BenchmarkRuntimeContendedRoots(b *testing.B) {
	m := statemachine.MustCompile([]statemachine.Transition[int, int, struct{}]{{From: 0, Event: 0, To: 0}})
	for _, capacity := range []int{1, 16, 1024} {
		b.Run(fmt.Sprintf("capacity=%d", capacity), func(b *testing.B) {
			r, err := queued.NewWithLimits(m, 0, queued.Limits{MaxRoots: capacity, MaxRunEvents: 1})
			if err != nil {
				b.Fatal(err)
			}
			var accepted, rejected atomic.Int64
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, err := r.Fire(context.Background(), 0, struct{}{})
					switch {
					case err == nil:
						accepted.Add(1)
					case errors.Is(err, queued.ErrRootLimit):
						rejected.Add(1)
					default:
						b.Error(err)
					}
				}
			})
			b.ReportMetric(float64(accepted.Load())/float64(b.N), "accepted/op")
			b.ReportMetric(float64(rejected.Load())/float64(b.N), "rejected/op")
			if n := accepted.Load(); n != 0 {
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(n), "ns/commit")
			}
		})
	}
}
