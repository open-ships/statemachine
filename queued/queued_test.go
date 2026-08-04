package queued_test

import (
	"context"
	"errors"
	"maps"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

type (
	state int
	event string
)

const (
	start  event = "start"
	first  event = "first"
	second event = "second"
	after  event = "after"
	fail   event = "fail"
	never  event = "never"
)

type command struct {
	value string
}

type row = statemachine.Transition[state, event, *command]

func TestZeroRuntimeAndNilMachineAreUsable(t *testing.T) {
	var zero queued.Runtime[state, event, *command]
	got, err := zero.Fire(context.Background(), start, &command{})
	if got != 0 || !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Fatalf("zero Fire = (%v, %v), want (0, ErrNotPermitted)", got, err)
	}
	if got := maps.Collect(zero.Permitted(context.Background(), &command{})); len(got) != 0 {
		t.Fatalf("zero Permitted = %v, want empty", got)
	}

	r := queued.New[state, event, *command](nil, 7)
	got, err = r.Fire(context.Background(), start, &command{})
	if got != 7 || !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Fatalf("nil-machine Fire = (%v, %v), want (7, ErrNotPermitted)", got, err)
	}
	if r.State() != 7 {
		t.Fatalf("State = %v, want 7", r.State())
	}
}

func TestStatePublishesOnlyAfterEffectSucceeds(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var r *queued.Runtime[state, event, *command]
	m := statemachine.MustCompile([]row{{
		From: 0, Event: start, To: 1,
		Do: func(context.Context, *command) error {
			close(entered)
			if got := r.State(); got != 0 {
				t.Errorf("State during Do = %v, want last committed state 0", got)
			}
			<-release
			return nil
		},
	}})
	r = queued.New(m, state(0))

	type answer struct {
		state state
		err   error
	}
	done := make(chan answer, 1)
	go func() {
		got, err := r.Fire(context.Background(), start, &command{})
		done <- answer{got, err}
	}()

	await(t, entered)
	if got := r.State(); got != 0 {
		t.Fatalf("State while Do is blocked = %v, want 0", got)
	}
	close(release)
	result := await(t, done)
	if result.state != 1 || result.err != nil {
		t.Fatalf("Fire = (%v, %v), want (1, nil)", result.state, result.err)
	}
	if got := r.State(); got != 1 {
		t.Fatalf("State after Fire = %v, want 1", got)
	}
}

func TestEffectErrorLeavesStateUnchanged(t *testing.T) {
	errEffect := errors.New("effect failed")
	calls := 0
	m := statemachine.MustCompile([]row{{
		From: 3, Event: start, To: 4,
		Do: func(context.Context, *command) error {
			calls++
			return errEffect
		},
	}})
	r := queued.New(m, state(3))

	got, err := r.Fire(context.Background(), start, &command{})
	if got != 3 || err != errEffect {
		t.Fatalf("Fire = (%v, %v), want (3, original effect error)", got, err)
	}
	if r.State() != 3 || calls != 1 {
		t.Fatalf("State/calls = %v/%v, want 3/1", r.State(), calls)
	}
}

func TestPermittedIsAnEagerStateSnapshot(t *testing.T) {
	guardCalls := 0
	allowed := true
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Guard: func(context.Context, *command) error {
				guardCalls++
				if !allowed {
					return errors.New("closed")
				}
				return nil
			},
		},
		{From: 1, Event: after, To: 2},
	})
	r := queued.New(m, state(0))

	snapshot := r.Permitted(context.Background(), &command{})
	if guardCalls != 1 {
		t.Fatalf("guards called before ranging = %d, want 1", guardCalls)
	}
	allowed = false
	if _, err := r.Fire(context.Background(), start, &command{}); !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Fatalf("Fire after guard change = %v, want refusal", err)
	}

	want := map[event]state{start: 1}
	if got := maps.Collect(snapshot); !maps.Equal(got, want) {
		t.Fatalf("snapshot = %v, want %v", got, want)
	}
	if got := maps.Collect(snapshot); !maps.Equal(got, want) {
		t.Fatalf("second range = %v, want %v", got, want)
	}
	if guardCalls != 2 {
		t.Fatalf("guards called = %d, want 2 (snapshot plus Fire only)", guardCalls)
	}
}

func TestRootCascadesAndExternalRootsRunFIFO(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var order []event
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Do: func(ctx context.Context, data *command) error {
				order = append(order, start)
				close(entered)
				<-release
				if err := queued.Enqueue(ctx, first, data); err != nil {
					return err
				}
				return queued.Enqueue(ctx, second, data)
			},
		},
		{
			From: 1, Event: first, To: 2,
			Do: func(context.Context, *command) error {
				order = append(order, first)
				return nil
			},
		},
		{
			From: 2, Event: second, To: 3,
			Do: func(context.Context, *command) error {
				order = append(order, second)
				return nil
			},
		},
		{
			From: 3, Event: after, To: 4,
			Do: func(context.Context, *command) error {
				order = append(order, after)
				return nil
			},
		},
	})
	r := queued.New(m, state(0))

	firstDone := fireAsync(r, context.Background(), start)
	await(t, entered)
	secondDone := fireAsync(r, context.Background(), after)
	assertBlocked(t, secondDone)
	close(release)

	one := await(t, firstDone)
	two := await(t, secondDone)
	if one.state != 3 || one.err != nil {
		t.Fatalf("first root = (%v, %v), want (3, nil)", one.state, one.err)
	}
	if two.state != 4 || two.err != nil {
		t.Fatalf("second root = (%v, %v), want (4, nil)", two.state, two.err)
	}
	if want := []event{start, first, second, after}; !slices.Equal(order, want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
}

func TestRuntimeObservesEveryCommittedStepInOneRun(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request")
	rootData := &command{value: "root"}
	followData := &command{value: "follow"}
	var r *queued.Runtime[state, event, *command]
	var observations []statemachine.Observation[state, event]
	var values []string
	var observedStates []state
	var enqueueErrors []error
	var fireErrors []error
	var contexts []any
	panicCalls := 0
	goexitCalls := 0

	m := statemachine.MustCompile([]row{
		{From: 0, Event: start, To: 1, Do: func(ctx context.Context, _ *command) error {
			return queued.Enqueue(ctx, first, followData)
		}},
		{From: 1, Event: first, To: 2},
		{From: 2, Event: after, To: 3},
	})
	observer := func(observerCtx context.Context, observation statemachine.Observation[state, event], data *command) {
		observations = append(observations, observation)
		values = append(values, data.value)
		observedStates = append(observedStates, r.State())
		enqueueErrors = append(enqueueErrors, queued.Enqueue(observerCtx, never, data))
		_, err := r.Fire(observerCtx, after, data)
		fireErrors = append(fireErrors, err)
		contexts = append(contexts, observerCtx.Value(contextKey{}))
	}
	r = queued.NewWithObservers(
		m,
		state(0),
		func(context.Context, statemachine.Observation[state, event], *command) {
			panicCalls++
			panic("observer panic")
		},
		func(context.Context, statemachine.Observation[state, event], *command) {
			goexitCalls++
			runtime.Goexit()
		},
		observer,
	)

	if got, err := r.Fire(ctx, start, rootData); err != nil || got != 2 {
		t.Fatalf("root Fire = (%v, %v), want (2, nil)", got, err)
	}
	if got, err := r.Fire(ctx, after, rootData); err != nil || got != 3 {
		t.Fatalf("later Fire = (%v, %v), want (3, nil)", got, err)
	}
	want := []statemachine.Observation[state, event]{
		{Seq: 1, Step: 1, Run: 1, Remaining: 1, Move: statemachine.Exited, State: 0, Event: start},
		{Seq: 2, Step: 1, Run: 1, Move: statemachine.Entered, State: 1, Event: start},
		{Seq: 3, Step: 3, Run: 1, Remaining: 1, Move: statemachine.Exited, State: 1, Event: first},
		{Seq: 4, Step: 3, Run: 1, Move: statemachine.Entered, State: 2, Event: first},
		{Seq: 5, Step: 5, Run: 5, Remaining: 1, Move: statemachine.Exited, State: 2, Event: after},
		{Seq: 6, Step: 5, Run: 5, Move: statemachine.Entered, State: 3, Event: after},
	}
	if !slices.Equal(observations, want) {
		t.Fatalf("observations = %+v, want %+v", observations, want)
	}
	if !slices.Equal(values, []string{"root", "root", "follow", "follow", "root", "root"}) {
		t.Fatalf("observer data = %v", values)
	}
	if !slices.Equal(observedStates, []state{1, 1, 2, 2, 3, 3}) {
		t.Fatalf("observer states = %v", observedStates)
	}
	if panicCalls != len(want) || goexitCalls != len(want) {
		t.Fatalf("failed observer calls = panic %d, Goexit %d; want %d each", panicCalls, goexitCalls, len(want))
	}
	for index := range observations {
		if enqueueErrors[index] != queued.ErrNotRunning || fireErrors[index] != queued.ErrReentrant || contexts[index] != "request" {
			t.Errorf("observer[%d] enqueue/fire/context = %v/%v/%v", index, enqueueErrors[index], fireErrors[index], contexts[index])
		}
	}
}

func TestRuntimeSilentSelfRootAnchorsRunAtFirstChangingFollowup(t *testing.T) {
	selfCalls := 0
	var observations []statemachine.Observation[state, event]
	m := statemachine.MustCompile([]row{
		{From: 0, Event: start, To: 0, Do: func(ctx context.Context, data *command) error {
			selfCalls++
			return queued.Enqueue(ctx, first, data)
		}},
		{From: 0, Event: first, To: 1},
	})
	r := queued.NewWithObservers(m, state(0), func(
		_ context.Context, observation statemachine.Observation[state, event], _ *command,
	) {
		observations = append(observations, observation)
	})

	if got, err := r.Fire(context.Background(), start, &command{}); err != nil || got != 1 {
		t.Fatalf("Fire = (%v, %v), want (1, nil)", got, err)
	}
	want := []statemachine.Observation[state, event]{
		{Seq: 1, Step: 1, Run: 1, Remaining: 1, Move: statemachine.Exited, State: 0, Event: first},
		{Seq: 2, Step: 1, Run: 1, Move: statemachine.Entered, State: 1, Event: first},
	}
	if selfCalls != 1 || !slices.Equal(observations, want) {
		t.Fatalf("self calls/observations = %d/%+v", selfCalls, observations)
	}
}

func TestReentrantFireIsRejectedAndEnqueueContextExpires(t *testing.T) {
	var r *queued.Runtime[state, event, *command]
	var nestedState state
	var nestedErr error
	var executionCtx context.Context
	m := statemachine.MustCompile([]row{{
		From: 5, Event: start, To: 6,
		Do: func(ctx context.Context, data *command) error {
			executionCtx = ctx
			nestedState, nestedErr = r.Fire(ctx, after, data)
			return nil
		},
	}})
	r = queued.New(m, state(5))

	got, err := r.Fire(context.Background(), start, &command{})
	if got != 6 || err != nil {
		t.Fatalf("outer Fire = (%v, %v), want (6, nil)", got, err)
	}
	if nestedState != 5 || nestedErr != queued.ErrReentrant {
		t.Fatalf("nested Fire = (%v, %v), want (5, ErrReentrant)", nestedState, nestedErr)
	}
	if err := queued.Enqueue(context.Background(), after, &command{}); err != queued.ErrNotRunning {
		t.Fatalf("Enqueue with ordinary context = %v, want ErrNotRunning", err)
	}
	if err := queued.Enqueue(executionCtx, after, &command{}); err != queued.ErrNotRunning {
		t.Fatalf("Enqueue with expired context = %v, want ErrNotRunning", err)
	}
	if _, err := r.Fire(executionCtx, after, &command{}); !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Fatalf("Fire with expired execution context = %v, want ordinary refusal", err)
	}
}

func TestSynchronousFireFromCallbackRejectsEveryQueuedRuntime(t *testing.T) {
	var b *queued.Runtime[state, event, *command]
	var nestedState state
	var nestedErr error
	machineA := statemachine.MustCompile([]row{{
		From: 0, Event: start, To: 1,
		Do: func(ctx context.Context, data *command) error {
			nestedState, nestedErr = b.Fire(ctx, start, data)
			return nil
		},
	}})
	machineB := statemachine.MustCompile([]row{{
		From: 10, Event: start, To: 11,
	}})
	a := queued.New(machineA, state(0))
	b = queued.New(machineB, state(10))

	got := await(t, fireAsync(a, context.Background(), start))
	if got.state != 1 || got.err != nil {
		t.Fatalf("outer Fire = (%v, %v), want (1, nil)", got.state, got.err)
	}
	if nestedState != 10 || nestedErr != queued.ErrReentrant {
		t.Fatalf("nested Fire = (%v, %v), want (10, ErrReentrant)", nestedState, nestedErr)
	}
	if b.State() != 10 {
		t.Fatalf("nested runtime state = %v, want 10", b.State())
	}
}

func TestCanceledRootIsSkippedWhenItsTurnArrives(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var skippedCalls atomic.Int32
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Do: func(context.Context, *command) error {
				close(entered)
				<-release
				return nil
			},
		},
		{
			From: 1, Event: after, To: 2,
			Do: func(context.Context, *command) error {
				skippedCalls.Add(1)
				return nil
			},
		},
	})
	r := queued.New(m, state(0))

	firstDone := fireAsync(r, context.Background(), start)
	await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := fireAsync(r, ctx, after)
	cancel()
	close(release)

	if got := await(t, firstDone); got.state != 1 || got.err != nil {
		t.Fatalf("first root = (%v, %v), want (1, nil)", got.state, got.err)
	}
	got := await(t, secondDone)
	if got.state != 1 || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("canceled root = (%v, %v), want (1, context.Canceled)", got.state, got.err)
	}
	if skippedCalls.Load() != 0 || r.State() != 1 {
		t.Fatalf("skipped calls/state = %v/%v, want 0/1", skippedCalls.Load(), r.State())
	}
}

func TestCanceledFollowupAbortsOnlyItsRoot(t *testing.T) {
	var followCalls atomic.Int32
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Do: func(ctx context.Context, data *command) error {
				child, cancel := context.WithCancel(ctx)
				defer cancel()
				if err := queued.Enqueue(child, first, data); err != nil {
					return err
				}
				return nil
			},
		},
		{
			From: 1, Event: first, To: 2,
			Do: func(context.Context, *command) error {
				followCalls.Add(1)
				return nil
			},
		},
		{From: 1, Event: after, To: 3},
	})
	r := queued.New(m, state(0))

	got, err := r.Fire(context.Background(), start, &command{})
	if got != 1 || !errors.Is(err, context.Canceled) {
		t.Fatalf("root = (%v, %v), want (1, context.Canceled)", got, err)
	}
	if followCalls.Load() != 0 {
		t.Fatalf("canceled follow-up ran %d times", followCalls.Load())
	}
	got, err = r.Fire(context.Background(), after, &command{})
	if got != 3 || err != nil {
		t.Fatalf("later root = (%v, %v), want (3, nil)", got, err)
	}
}

func TestErrorDropsFollowupsWithoutStrandingLaterRoot(t *testing.T) {
	errEffect := errors.New("boom")
	failing := make(chan struct{})
	release := make(chan struct{})
	var neverCalls atomic.Int32
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Do: func(ctx context.Context, data *command) error {
				if err := queued.Enqueue(ctx, fail, data); err != nil {
					return err
				}
				return queued.Enqueue(ctx, never, data)
			},
		},
		{
			From: 1, Event: fail, To: 2,
			Do: func(context.Context, *command) error {
				close(failing)
				<-release
				return errEffect
			},
		},
		{
			From: 1, Event: never, To: 9,
			Do: func(context.Context, *command) error {
				neverCalls.Add(1)
				return nil
			},
		},
		{From: 1, Event: after, To: 3},
	})
	r := queued.New(m, state(0))

	rootDone := fireAsync(r, context.Background(), start)
	await(t, failing)
	laterDone := fireAsync(r, context.Background(), after)
	close(release)

	rootResult := await(t, rootDone)
	if rootResult.state != 1 || rootResult.err != errEffect {
		t.Fatalf("failed root = (%v, %v), want (1, original error)", rootResult.state, rootResult.err)
	}
	laterResult := await(t, laterDone)
	if laterResult.state != 3 || laterResult.err != nil {
		t.Fatalf("later root = (%v, %v), want (3, nil)", laterResult.state, laterResult.err)
	}
	if neverCalls.Load() != 0 {
		t.Fatalf("event after failure ran %d times", neverCalls.Load())
	}
}

func TestPanicIsReplayedDropsFollowupsAndLaterRootRuns(t *testing.T) {
	panicValue := &struct{ message string }{"transition panic"}
	panicking := make(chan struct{})
	release := make(chan struct{})
	var neverCalls atomic.Int32
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Do: func(ctx context.Context, data *command) error {
				if err := queued.Enqueue(ctx, fail, data); err != nil {
					return err
				}
				return queued.Enqueue(ctx, never, data)
			},
		},
		{
			From: 1, Event: fail, To: 2,
			Do: func(context.Context, *command) error {
				close(panicking)
				<-release
				panic(panicValue)
			},
		},
		{
			From: 1, Event: never, To: 9,
			Do: func(context.Context, *command) error {
				neverCalls.Add(1)
				return nil
			},
		},
		{From: 1, Event: after, To: 3},
	})
	r := queued.New(m, state(0))

	panicDone := make(chan any, 1)
	go func() {
		defer func() { panicDone <- recover() }()
		_, _ = r.Fire(context.Background(), start, &command{})
	}()
	await(t, panicking)
	laterDone := fireAsync(r, context.Background(), after)
	close(release)

	if recovered := await(t, panicDone); recovered != panicValue {
		t.Fatalf("recovered = %#v, want original panic value %#v", recovered, panicValue)
	}
	laterResult := await(t, laterDone)
	if laterResult.state != 3 || laterResult.err != nil {
		t.Fatalf("later root = (%v, %v), want (3, nil)", laterResult.state, laterResult.err)
	}
	if neverCalls.Load() != 0 || r.State() != 3 {
		t.Fatalf("never calls/state = %v/%v, want 0/3", neverCalls.Load(), r.State())
	}
}

func TestGoexitAbortsRootWithoutStrandingLaterRoot(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	m := statemachine.MustCompile([]row{
		{
			From: 0, Event: start, To: 1,
			Do: func(context.Context, *command) error {
				close(entered)
				<-release
				runtime.Goexit()
				return nil
			},
		},
		{From: 0, Event: after, To: 2},
	})
	r := queued.New(m, state(0))

	stopped := fireAsync(r, context.Background(), start)
	await(t, entered)
	later := fireAsync(r, context.Background(), after)
	close(release)

	firstResult := await(t, stopped)
	if firstResult.state != 0 || firstResult.err != queued.ErrExecutionStopped {
		t.Fatalf("Goexit root = (%v, %v), want (0, ErrExecutionStopped)", firstResult.state, firstResult.err)
	}
	laterResult := await(t, later)
	if laterResult.state != 2 || laterResult.err != nil {
		t.Fatalf("later root = (%v, %v), want (2, nil)", laterResult.state, laterResult.err)
	}
}

func TestConcurrentRootsAreSerialized(t *testing.T) {
	const count = 200
	var calls atomic.Int32
	var inFlight atomic.Int32
	var overlap atomic.Bool
	m := statemachine.MustCompile([]row{{
		From: 0, Event: start, To: 0,
		Do: func(context.Context, *command) error {
			if inFlight.Add(1) != 1 {
				overlap.Store(true)
			}
			runtime.Gosched()
			calls.Add(1)
			inFlight.Add(-1)
			return nil
		},
	}})
	r := queued.New(m, state(0))

	var wg sync.WaitGroup
	errs := make(chan error, count)
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := r.Fire(context.Background(), start, &command{})
			if err != nil {
				errs <- err
				return
			}
			if got != 0 {
				errs <- errors.New("unexpected state")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if overlap.Load() {
		t.Error("two effects overlapped")
	}
	if calls.Load() != count {
		t.Fatalf("effect calls = %d, want %d", calls.Load(), count)
	}
}

func TestEnqueueSupportsRuntimeWithInterfaceData(t *testing.T) {
	type anyRow = statemachine.Transition[state, event, any]
	var seen []any
	m := statemachine.MustCompile([]anyRow{
		{
			From: 0, Event: start, To: 1,
			Do: func(ctx context.Context, _ any) error {
				// T is inferred as string here; it is still assignable to the
				// Runtime's any data type.
				return queued.Enqueue(ctx, first, "follow-up")
			},
		},
		{
			From: 1, Event: first, To: 2,
			Do: func(ctx context.Context, data any) error {
				seen = append(seen, data)
				return queued.Enqueue[event, any](ctx, second, nil)
			},
		},
		{
			From: 2, Event: second, To: 3,
			Do: func(_ context.Context, data any) error {
				seen = append(seen, data)
				return nil
			},
		},
	})
	r := queued.New(m, state(0))

	got, err := r.Fire(context.Background(), start, nil)
	if got != 3 || err != nil {
		t.Fatalf("Fire = (%v, %v), want (3, nil)", got, err)
	}
	if len(seen) != 2 || seen[0] != "follow-up" || seen[1] != nil {
		t.Fatalf("follow-up data = %#v, want []any{%q, nil}", seen, "follow-up")
	}
}

func TestEnqueueRejectsNilContextAndMismatchedTypes(t *testing.T) {
	if err := queued.Enqueue[event, *command](nil, first, nil); err != queued.ErrNotRunning {
		t.Fatalf("Enqueue with nil context = %v, want ErrNotRunning", err)
	}

	for _, test := range []struct {
		name string
		do   func(context.Context, *command) error
	}{
		{
			name: "event",
			do: func(ctx context.Context, data *command) error {
				return queued.Enqueue(ctx, string(first), data)
			},
		},
		{
			name: "data",
			do: func(ctx context.Context, _ *command) error {
				return queued.Enqueue(ctx, first, command{})
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := statemachine.MustCompile([]row{{
				From: 0, Event: start, To: 1, Do: test.do,
			}})
			r := queued.New(m, state(0))
			got, err := r.Fire(context.Background(), start, &command{})
			if got != 0 || err != queued.ErrNotRunning {
				t.Fatalf("Fire = (%v, %v), want (0, ErrNotRunning)", got, err)
			}
		})
	}
}

type fireResult struct {
	state state
	err   error
}

func fireAsync(r *queued.Runtime[state, event, *command], ctx context.Context, ev event) <-chan fireResult {
	done := make(chan fireResult, 1)
	go func() {
		got, err := r.Fire(ctx, ev, &command{})
		done <- fireResult{got, err}
	}()
	return done
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting")
		var zero T
		return zero
	}
}

func assertBlocked[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case value := <-ch:
		t.Fatalf("completed early with %v", value)
	case <-time.After(20 * time.Millisecond):
	}
}

var benchmarkRuntimeMachine = statemachine.MustCompile([]row{
	{From: 0, Event: start, To: 1},
	{From: 1, Event: after, To: 0},
})

func BenchmarkRuntimeFire(b *testing.B) {
	r := queued.New(benchmarkRuntimeMachine, state(0))
	data := &command{}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = r.Fire(context.Background(), start, data)
		_, _ = r.Fire(context.Background(), after, data)
	}
}

func BenchmarkRuntimeFireObserved(b *testing.B) {
	r := queued.NewWithObservers(
		benchmarkRuntimeMachine,
		state(0),
		func(context.Context, statemachine.Observation[state, event], *command) {},
	)
	data := &command{}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = r.Fire(context.Background(), start, data)
		_, _ = r.Fire(context.Background(), after, data)
	}
}
