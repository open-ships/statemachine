package queued

import (
	"context"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
)

func TestWorkerStopsWheneverQueueBecomesIdle(t *testing.T) {
	type (
		state int
		event int
	)
	m := statemachine.MustCompile([]statemachine.Transition[state, event, struct{}]{
		{From: 0, Event: 1, To: 0},
	})
	r := New(m, state(0))

	for range 20 {
		if _, err := r.Fire(context.Background(), 1, struct{}{}); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(5 * time.Second)
		for {
			r.mu.Lock()
			running := r.running
			roots := len(r.roots)
			r.mu.Unlock()
			if !running && roots == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("worker remained running with %d roots", roots)
			}
			time.Sleep(time.Millisecond)
		}
	}
}
