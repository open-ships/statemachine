package queued_test

import (
	"context"
	"errors"
	goruntime "runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

func TestCanceledQueuedRootIsRemovedBeforeItStarts(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var canceledCalls atomic.Int64
	machine := statemachine.MustCompile([]row{
		{From: 0, Event: start, To: 1, Do: func(context.Context, *command) error {
			close(entered)
			<-release
			return nil
		}},
		{From: 1, Event: after, To: 2, Do: func(context.Context, *command) error {
			canceledCalls.Add(1)
			return nil
		}},
	})
	runtime := queued.New(machine, state(0))
	first := fireAsync(runtime, context.Background(), start)
	await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	second := fireAsync(runtime, ctx, after)
	deadline := time.Now().Add(time.Second)
	for runtime.Status().OutstandingRoots != 2 {
		if time.Now().After(deadline) {
			t.Fatal("second root was not admitted")
		}
		goruntime.Gosched()
	}
	cancel()
	result := await(t, second)
	if !errors.Is(result.err, context.Canceled) || canceledCalls.Load() != 0 {
		t.Fatalf("canceled root = %+v, calls = %d", result, canceledCalls.Load())
	}
	close(release)
	if result := await(t, first); result.err != nil || result.state != 1 {
		t.Fatalf("first root = %+v", result)
	}
	if runtime.State() != 1 || canceledCalls.Load() != 0 {
		t.Fatalf("state/calls = %v/%d", runtime.State(), canceledCalls.Load())
	}
}

func TestRuntimeBoundEnqueueRejectsWrongRuntime(t *testing.T) {
	var a, b *queued.Runtime[state, event, *command]
	machine := statemachine.MustCompile([]row{
		{From: 0, Event: start, To: 1, Do: func(ctx context.Context, data *command) error {
			if err := b.Enqueue(ctx, first, data); !errors.Is(err, queued.ErrWrongRuntime) {
				return err
			}
			return a.Enqueue(ctx, first, data)
		}},
		{From: 1, Event: first, To: 2},
	})
	a = queued.New(machine, state(0))
	b = queued.New(machine, state(0))
	if got, err := a.Fire(context.Background(), start, &command{}); err != nil || got != 2 {
		t.Fatalf("Fire = %v, %v", got, err)
	}
	if b.State() != 0 {
		t.Fatalf("wrong Runtime changed to %v", b.State())
	}
}

func TestRuntimeResourceLimitsBoundRootsAndRunWork(t *testing.T) {
	t.Run("roots", func(t *testing.T) {
		entered := make(chan struct{})
		release := make(chan struct{})
		machine := statemachine.MustCompile([]row{{
			From: 0, Event: start, To: 0, Do: func(context.Context, *command) error {
				select {
				case <-entered:
				default:
					close(entered)
				}
				<-release
				return nil
			},
		}})
		runtime, err := queued.NewWithLimits(machine, state(0), queued.Limits{MaxRoots: 1, MaxRunEvents: 1})
		if err != nil {
			t.Fatal(err)
		}
		firstRun := fireAsync(runtime, context.Background(), start)
		await(t, entered)
		if _, err := runtime.Fire(context.Background(), start, &command{}); !errors.Is(err, queued.ErrRootLimit) {
			t.Fatalf("second root = %v", err)
		}
		close(release)
		_ = await(t, firstRun)
	})

	t.Run("cumulative run events", func(t *testing.T) {
		var runtime *queued.Runtime[state, event, *command]
		calls := 0
		machine := statemachine.MustCompile([]row{
			{From: 0, Event: start, To: 0, Do: func(ctx context.Context, data *command) error {
				calls++
				return runtime.Enqueue(ctx, start, data)
			}},
			{From: 0, Event: after, To: 1},
		})
		var err error
		runtime, err = queued.NewWithLimits(machine, state(0), queued.Limits{MaxRoots: 2, MaxRunEvents: 3})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.Fire(context.Background(), start, &command{}); !errors.Is(err, queued.ErrRunLimit) || calls != 3 {
			t.Fatalf("bounded Run = %v, calls %d", err, calls)
		}
		if got, err := runtime.Fire(context.Background(), after, &command{}); err != nil || got != 1 {
			t.Fatalf("later root = %v, %v", got, err)
		}
	})
}

func TestRuntimeOwnsCompiledMachineValue(t *testing.T) {
	original := statemachine.MustCompile([]row{{From: 0, Event: start, To: 1}})
	runtime := queued.New(original, state(0))
	replacement := statemachine.MustCompile([]row{{From: 0, Event: start, To: 9}})
	*original = *replacement
	if got, err := runtime.Fire(context.Background(), start, &command{}); err != nil || got != 1 {
		t.Fatalf("Fire after caller overwrite = %v, %v", got, err)
	}
}

func TestNewWithLimitsRejectsInvalidLimits(t *testing.T) {
	if _, err := queued.NewWithLimits[state, event, *command](nil, 0, queued.Limits{}); !errors.Is(err, queued.ErrInvalidLimits) {
		t.Fatalf("NewWithLimits = %v", err)
	}
}
