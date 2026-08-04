package statemachine_test

import (
	"context"
	"errors"
	"iter"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
)

type instanceState string

const (
	instanceIdle   instanceState = "idle"
	instanceActive instanceState = "active"
	instanceDone   instanceState = "done"
)

type instanceEvent string

const (
	instanceStart   instanceEvent = "start"
	instanceFinish  instanceEvent = "finish"
	instanceFail    instanceEvent = "fail"
	instanceBlocked instanceEvent = "blocked"
	instancePing    instanceEvent = "ping"
	instanceBoom    instanceEvent = "boom"
)

type instanceData struct{}

type instanceRow = statemachine.Transition[instanceState, instanceEvent, *instanceData]

type instanceChoice struct {
	event instanceEvent
	to    instanceState
}

func collectInstanceChoices(seq iter.Seq2[instanceEvent, instanceState]) []instanceChoice {
	var choices []instanceChoice
	for event, to := range seq {
		choices = append(choices, instanceChoice{event: event, to: to})
	}
	return choices
}

func TestInstancePublishesStateOnlyAfterEffectSucceeds(t *testing.T) {
	var instance *statemachine.Instance[instanceState, instanceEvent, *instanceData]
	var observed []instanceState
	m := statemachine.MustCompile([]instanceRow{
		{
			From:  instanceIdle,
			Event: instanceStart,
			To:    instanceActive,
			Guard: func(context.Context, *instanceData) error {
				observed = append(observed, instance.State())
				return nil
			},
			Do: func(context.Context, *instanceData) error {
				observed = append(observed, instance.State())
				return nil
			},
		},
	})
	instance = statemachine.NewInstance(m, instanceIdle)

	got, err := instance.Fire(context.Background(), instanceStart, &instanceData{})
	if err != nil {
		t.Fatalf("Fire: %v", err)
	}
	if got != instanceActive {
		t.Errorf("Fire state = %v, want %v", got, instanceActive)
	}
	if state := instance.State(); state != instanceActive {
		t.Errorf("State = %v, want %v", state, instanceActive)
	}
	if want := []instanceState{instanceIdle, instanceIdle}; !slices.Equal(observed, want) {
		t.Errorf("states observed by Guard and Do = %v, want %v", observed, want)
	}
}

func TestInstanceFailuresPreserveStateAndError(t *testing.T) {
	errGuard := errors.New("guard declined")
	errEffect := errors.New("effect failed")
	var effectCalls int
	m := statemachine.MustCompile([]instanceRow{
		{
			From:  instanceIdle,
			Event: instanceBlocked,
			To:    instanceActive,
			Guard: func(context.Context, *instanceData) error { return errGuard },
		},
		{
			From:  instanceIdle,
			Event: instanceFail,
			To:    instanceActive,
			Do: func(context.Context, *instanceData) error {
				effectCalls++
				return errEffect
			},
		},
	})
	instance := statemachine.NewInstance(m, instanceIdle)

	got, err := instance.Fire(context.Background(), instanceBlocked, &instanceData{})
	if got != instanceIdle {
		t.Errorf("state after refusal = %v, want %v", got, instanceIdle)
	}
	if !errors.Is(err, statemachine.ErrNotPermitted) || !errors.Is(err, errGuard) {
		t.Errorf("refusal = %v, want ErrNotPermitted and the Guard reason", err)
	}
	if state := instance.State(); state != instanceIdle {
		t.Errorf("State after refusal = %v, want %v", state, instanceIdle)
	}

	got, err = instance.Fire(context.Background(), instanceFail, &instanceData{})
	if got != instanceIdle {
		t.Errorf("state after effect failure = %v, want %v", got, instanceIdle)
	}
	if err != errEffect { //nolint:errorlint // Instance must preserve exact error identity.
		t.Errorf("effect error = %#v, want exact value %#v", err, errEffect)
	}
	if effectCalls != 1 {
		t.Errorf("effect calls = %d, want 1", effectCalls)
	}
	if state := instance.State(); state != instanceIdle {
		t.Errorf("State after effect failure = %v, want %v", state, instanceIdle)
	}
}

func TestInstanceRejectsRecursiveFire(t *testing.T) {
	var instance *statemachine.Instance[instanceState, instanceEvent, *instanceData]
	var nestedState instanceState
	var nestedErr error
	var nestedEffects int
	m := statemachine.MustCompile([]instanceRow{
		{
			From:  instanceIdle,
			Event: instanceStart,
			To:    instanceActive,
			Do: func(ctx context.Context, data *instanceData) error {
				nestedState, nestedErr = instance.Fire(ctx, instanceFinish, data)
				return nil
			},
		},
		{
			From:  instanceIdle,
			Event: instanceFinish,
			To:    instanceDone,
			Do: func(context.Context, *instanceData) error {
				nestedEffects++
				return nil
			},
		},
	})
	instance = statemachine.NewInstance(m, instanceIdle)

	got, err := instance.Fire(context.Background(), instanceStart, &instanceData{})
	if err != nil {
		t.Fatalf("outer Fire: %v", err)
	}
	if got != instanceActive {
		t.Errorf("outer state = %v, want %v", got, instanceActive)
	}
	if nestedState != instanceIdle {
		t.Errorf("recursive state = %v, want last committed state %v", nestedState, instanceIdle)
	}
	if nestedErr != statemachine.ErrInFlight {
		t.Errorf("recursive error = %v, want exact ErrInFlight", nestedErr)
	}
	if nestedEffects != 0 {
		t.Errorf("recursive effect calls = %d, want 0", nestedEffects)
	}
}

func TestInstanceRejectsConcurrentFireWithoutHoldingItsLock(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	m := statemachine.MustCompile([]instanceRow{
		{
			From:  instanceIdle,
			Event: instanceStart,
			To:    instanceActive,
			Do: func(context.Context, *instanceData) error {
				calls.Add(1)
				close(started)
				<-release
				return nil
			},
		},
		{From: instanceIdle, Event: instancePing, To: instanceIdle},
	})
	instance := statemachine.NewInstance(m, instanceIdle)

	type result struct {
		state instanceState
		err   error
	}
	outer := make(chan result, 1)
	go func() {
		state, err := instance.Fire(context.Background(), instanceStart, &instanceData{})
		outer <- result{state: state, err: err}
	}()
	<-started

	stateResult := make(chan instanceState, 1)
	go func() { stateResult <- instance.State() }()
	select {
	case state := <-stateResult:
		if state != instanceIdle {
			t.Errorf("State during Do = %v, want %v", state, instanceIdle)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("State blocked while Do was running")
	}

	choicesResult := make(chan []instanceChoice, 1)
	go func() {
		choicesResult <- collectInstanceChoices(instance.Permitted(context.Background(), &instanceData{}))
	}()
	select {
	case choices := <-choicesResult:
		want := []instanceChoice{
			{event: instanceStart, to: instanceActive},
			{event: instancePing, to: instanceIdle},
		}
		if !slices.Equal(choices, want) {
			t.Errorf("Permitted during Do = %v, want snapshot %v", choices, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Permitted blocked while Do was running")
	}

	const contenders = 32
	begin := make(chan struct{})
	results := make(chan result, contenders)
	var group sync.WaitGroup
	group.Add(contenders)
	for range contenders {
		go func() {
			defer group.Done()
			<-begin
			state, err := instance.Fire(context.Background(), instancePing, &instanceData{})
			results <- result{state: state, err: err}
		}()
	}
	close(begin)
	group.Wait()
	close(results)
	for result := range results {
		if result.state != instanceIdle {
			t.Errorf("overlapping state = %v, want %v", result.state, instanceIdle)
		}
		if result.err != statemachine.ErrInFlight {
			t.Errorf("overlapping error = %v, want exact ErrInFlight", result.err)
		}
	}

	close(release)
	outerResult := <-outer
	if outerResult.err != nil {
		t.Fatalf("outer Fire: %v", outerResult.err)
	}
	if outerResult.state != instanceActive {
		t.Errorf("outer state = %v, want %v", outerResult.state, instanceActive)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("effect calls = %d, want 1", got)
	}
}

func TestInstancePanicClearsInFlightAndPreservesState(t *testing.T) {
	panicValue := &struct{ message string }{"effect panic"}
	var partialEffect bool
	m := statemachine.MustCompile([]instanceRow{
		{
			From:  instanceIdle,
			Event: instanceBoom,
			To:    instanceActive,
			Do: func(context.Context, *instanceData) error {
				partialEffect = true
				panic(panicValue)
			},
		},
		{From: instanceIdle, Event: instanceStart, To: instanceActive},
	})
	instance := statemachine.NewInstance(m, instanceIdle)

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = instance.Fire(context.Background(), instanceBoom, &instanceData{})
	}()
	if recovered != panicValue {
		t.Errorf("recovered = %#v, want exact panic value %#v", recovered, panicValue)
	}
	if !partialEffect {
		t.Error("effect before panic did not remain observable")
	}
	if state := instance.State(); state != instanceIdle {
		t.Errorf("State after panic = %v, want %v", state, instanceIdle)
	}

	got, err := instance.Fire(context.Background(), instanceStart, &instanceData{})
	if err != nil {
		t.Fatalf("Fire after panic: %v", err)
	}
	if got != instanceActive {
		t.Errorf("state after recovery = %v, want %v", got, instanceActive)
	}
}

func TestInstanceGoexitClearsInFlightAndPreservesState(t *testing.T) {
	m := statemachine.MustCompile([]instanceRow{
		{
			From: instanceIdle, Event: instanceBoom, To: instanceActive,
			Do: func(context.Context, *instanceData) error {
				runtime.Goexit()
				return nil
			},
		},
		{From: instanceIdle, Event: instanceStart, To: instanceActive},
	})
	instance := statemachine.NewInstance(m, instanceIdle)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = instance.Fire(context.Background(), instanceBoom, &instanceData{})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Goexit callback did not finish")
	}
	if got := instance.State(); got != instanceIdle {
		t.Fatalf("State after Goexit = %v, want %v", got, instanceIdle)
	}
	if got, err := instance.Fire(context.Background(), instanceStart, &instanceData{}); err != nil || got != instanceActive {
		t.Fatalf("Fire after Goexit = (%v, %v), want (%v, nil)", got, err, instanceActive)
	}
}

func TestInstanceSelfTransitionRunsEveryTime(t *testing.T) {
	var calls int
	m := statemachine.MustCompile([]instanceRow{
		{
			From:  instanceIdle,
			Event: instancePing,
			To:    instanceIdle,
			Do: func(context.Context, *instanceData) error {
				calls++
				return nil
			},
		},
	})
	instance := statemachine.NewInstance(m, instanceIdle)

	for range 2 {
		got, err := instance.Fire(context.Background(), instancePing, &instanceData{})
		if err != nil {
			t.Fatalf("Fire: %v", err)
		}
		if got != instanceIdle {
			t.Errorf("self-transition state = %v, want %v", got, instanceIdle)
		}
	}
	if calls != 2 {
		t.Errorf("effect calls = %d, want 2", calls)
	}
}

func TestInstanceObservesCommittedPositionChanges(t *testing.T) {
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "request")
	data := &instanceData{}
	var instance *statemachine.Instance[instanceState, instanceEvent, *instanceData]
	var observations []statemachine.Observation[instanceState, instanceEvent]
	var observedStates []instanceState
	var contexts []any
	var observedData []*instanceData
	var nestedErrors []error

	observer := func(
		observerCtx context.Context,
		observation statemachine.Observation[instanceState, instanceEvent],
		observed *instanceData,
	) {
		observations = append(observations, observation)
		observedStates = append(observedStates, instance.State())
		contexts = append(contexts, observerCtx.Value(contextKey{}))
		observedData = append(observedData, observed)
		_, err := instance.Fire(observerCtx, instanceFinish, observed)
		nestedErrors = append(nestedErrors, err)
	}

	selfCalls := 0
	m := statemachine.MustCompile([]instanceRow{
		{From: instanceIdle, Event: instanceStart, To: instanceActive},
		{From: instanceActive, Event: instancePing, To: instanceActive, Do: func(context.Context, *instanceData) error {
			selfCalls++
			return nil
		}},
		{From: instanceActive, Event: instanceFinish, To: instanceDone},
	})
	instance = statemachine.NewInstanceWithObservers(m, instanceIdle, nil, observer)

	if got, err := instance.Fire(ctx, instanceStart, data); err != nil || got != instanceActive {
		t.Fatalf("start = (%v, %v)", got, err)
	}
	if got, err := instance.Fire(ctx, instancePing, data); err != nil || got != instanceActive {
		t.Fatalf("self = (%v, %v)", got, err)
	}
	if got, err := instance.Fire(ctx, instanceFinish, data); err != nil || got != instanceDone {
		t.Fatalf("finish = (%v, %v)", got, err)
	}

	want := []statemachine.Observation[instanceState, instanceEvent]{
		{Seq: 1, Step: 1, Run: 1, Remaining: 1, Move: statemachine.Exited, State: instanceIdle, Event: instanceStart},
		{Seq: 2, Step: 1, Run: 1, Move: statemachine.Entered, State: instanceActive, Event: instanceStart},
		{Seq: 3, Step: 3, Run: 3, Remaining: 1, Move: statemachine.Exited, State: instanceActive, Event: instanceFinish},
		{Seq: 4, Step: 3, Run: 3, Move: statemachine.Entered, State: instanceDone, Event: instanceFinish},
	}
	if !slices.Equal(observations, want) {
		t.Fatalf("observations = %+v, want %+v", observations, want)
	}
	if !slices.Equal(observedStates, []instanceState{instanceActive, instanceActive, instanceDone, instanceDone}) {
		t.Fatalf("observer states = %v", observedStates)
	}
	for index := range observations {
		if contexts[index] != "request" || observedData[index] != data || nestedErrors[index] != statemachine.ErrInFlight {
			t.Errorf("callback[%d] = context %v, data %p, nested %v", index, contexts[index], observedData[index], nestedErrors[index])
		}
	}
	if selfCalls != 1 {
		t.Fatalf("self transition ran %d effects, want 1", selfCalls)
	}
}

func TestInstanceObserverFailuresAreIsolated(t *testing.T) {
	m := statemachine.MustCompile([]instanceRow{{From: instanceIdle, Event: instanceStart, To: instanceActive}})
	var delivered []statemachine.Observation[instanceState, instanceEvent]
	panicCalls := 0
	goexitCalls := 0
	combined := statemachine.Observers(
		func(context.Context, statemachine.Observation[instanceState, instanceEvent], *instanceData) {
			panic("observer panic")
		},
		func(context.Context, statemachine.Observation[instanceState, instanceEvent], *instanceData) {
			runtime.Goexit()
		},
		func(_ context.Context, observation statemachine.Observation[instanceState, instanceEvent], _ *instanceData) {
			delivered = append(delivered, observation)
		},
	)
	instance := statemachine.NewInstanceWithObservers(
		m,
		instanceIdle,
		func(context.Context, statemachine.Observation[instanceState, instanceEvent], *instanceData) {
			panicCalls++
			panic("direct observer panic")
		},
		func(context.Context, statemachine.Observation[instanceState, instanceEvent], *instanceData) {
			goexitCalls++
			runtime.Goexit()
		},
		combined,
	)

	got, err := instance.Fire(context.Background(), instanceStart, &instanceData{})
	if err != nil || got != instanceActive {
		t.Fatalf("Fire = (%v, %v), want (%v, nil)", got, err, instanceActive)
	}
	if len(delivered) != 2 || delivered[1].Remaining != 0 {
		t.Fatalf("delivered = %+v", delivered)
	}
	if panicCalls != 2 || goexitCalls != 2 {
		t.Fatalf("failed observer calls = panic %d, Goexit %d; want 2 each", panicCalls, goexitCalls)
	}
}

func TestInstancesOwnIndependentStateAndCopyMachineValue(t *testing.T) {
	m := statemachine.MustCompile([]instanceRow{
		{From: instanceIdle, Event: instanceStart, To: instanceActive},
	})
	first := statemachine.NewInstance(m, instanceIdle)
	second := statemachine.NewInstance(m, instanceIdle)

	// NewInstance copies the Machine value rather than retaining this pointer.
	*m = statemachine.Machine[instanceState, instanceEvent, *instanceData]{}

	got, err := first.Fire(context.Background(), instanceStart, &instanceData{})
	if err != nil {
		t.Fatalf("first Fire: %v", err)
	}
	if got != instanceActive {
		t.Errorf("first state = %v, want %v", got, instanceActive)
	}
	if got := second.State(); got != instanceIdle {
		t.Errorf("second State after first fired = %v, want %v", got, instanceIdle)
	}
	got, err = second.Fire(context.Background(), instanceStart, &instanceData{})
	if err != nil {
		t.Fatalf("second Fire: %v", err)
	}
	if got != instanceActive {
		t.Errorf("second state = %v, want %v", got, instanceActive)
	}
}

func TestNilAndZeroInstancesAreUsable(t *testing.T) {
	nilMachine := statemachine.NewInstance[instanceState, instanceEvent, *instanceData](nil, instanceActive)
	if got := nilMachine.State(); got != instanceActive {
		t.Errorf("nil-Machine State = %v, want %v", got, instanceActive)
	}
	got, err := nilMachine.Fire(context.Background(), instanceFinish, &instanceData{})
	if got != instanceActive {
		t.Errorf("nil-Machine Fire state = %v, want %v", got, instanceActive)
	}
	if !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Errorf("nil-Machine Fire error = %v, want ErrNotPermitted", err)
	}
	if choices := collectInstanceChoices(nilMachine.Permitted(context.Background(), &instanceData{})); choices != nil {
		t.Errorf("nil-Machine Permitted = %v, want nil", choices)
	}

	var zero statemachine.Instance[instanceState, instanceEvent, *instanceData]
	if got := zero.State(); got != "" {
		t.Errorf("zero Instance State = %v, want zero state", got)
	}
	got, err = zero.Fire(context.Background(), instanceStart, &instanceData{})
	if got != "" {
		t.Errorf("zero Instance Fire state = %v, want zero state", got)
	}
	if !errors.Is(err, statemachine.ErrNotPermitted) {
		t.Errorf("zero Instance Fire error = %v, want ErrNotPermitted", err)
	}
	if choices := collectInstanceChoices(zero.Permitted(context.Background(), &instanceData{})); choices != nil {
		t.Errorf("zero Instance Permitted = %v, want nil", choices)
	}
}

type permittedData struct {
	allowed map[instanceEvent]bool
	calls   *[]instanceEvent
}

func TestInstancePermittedIsAnEagerOrderedSnapshot(t *testing.T) {
	guard := func(event instanceEvent) func(context.Context, *permittedData) error {
		return func(_ context.Context, data *permittedData) error {
			if data.calls != nil {
				*data.calls = append(*data.calls, event)
			}
			if !data.allowed[event] {
				return errors.New("not allowed")
			}
			return nil
		}
	}
	type permittedRow = statemachine.Transition[instanceState, instanceEvent, *permittedData]
	m := statemachine.MustCompile([]permittedRow{
		{From: instanceIdle, Event: instanceStart, To: instanceActive, Guard: guard(instanceStart)},
		{From: instanceIdle, Event: instanceFinish, To: instanceDone, Guard: guard(instanceFinish)},
	})
	instance := statemachine.NewInstance(m, instanceIdle)
	calls := []instanceEvent{}
	data := &permittedData{
		allowed: map[instanceEvent]bool{instanceStart: true, instanceFinish: true},
		calls:   &calls,
	}

	snapshot := instance.Permitted(context.Background(), data)
	if want := []instanceEvent{instanceStart, instanceFinish}; !slices.Equal(calls, want) {
		t.Fatalf("Guards called before iteration = %v, want %v", calls, want)
	}
	var yielded int
	snapshot(func(instanceEvent, instanceState) bool {
		yielded++
		return false
	})
	if yielded != 1 {
		t.Errorf("choices yielded before stopping = %d, want 1", yielded)
	}
	if !slices.Equal(calls, []instanceEvent{instanceStart, instanceFinish}) {
		t.Errorf("stopping iteration changed Guard calls: %v", calls)
	}

	// Change both the owned state and the data after the snapshot was made.
	// Neither change may alter the already-returned iterator.
	got, err := instance.Fire(context.Background(), instanceStart, &permittedData{
		allowed: map[instanceEvent]bool{instanceStart: true},
	})
	if err != nil {
		t.Fatalf("Fire after snapshot: %v", err)
	}
	if got != instanceActive {
		t.Errorf("Fire state = %v, want %v", got, instanceActive)
	}
	data.allowed[instanceStart] = false
	data.allowed[instanceFinish] = false

	want := []instanceChoice{
		{event: instanceStart, to: instanceActive},
		{event: instanceFinish, to: instanceDone},
	}
	if choices := collectInstanceChoices(snapshot); !slices.Equal(choices, want) {
		t.Errorf("snapshot choices = %v, want %v", choices, want)
	}
	if !slices.Equal(calls, []instanceEvent{instanceStart, instanceFinish}) {
		t.Errorf("Guards ran again during iteration: calls = %v", calls)
	}
}

var benchmarkInstanceMachine = statemachine.MustCompile([]instanceRow{
	{From: instanceIdle, Event: instanceStart, To: instanceActive},
	{From: instanceActive, Event: instanceFinish, To: instanceIdle},
})

func BenchmarkInstanceFire(b *testing.B) {
	instance := statemachine.NewInstance(benchmarkInstanceMachine, instanceIdle)
	data := &instanceData{}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = instance.Fire(context.Background(), instanceStart, data)
		_, _ = instance.Fire(context.Background(), instanceFinish, data)
	}
}

func BenchmarkInstanceFireObserved(b *testing.B) {
	instance := statemachine.NewInstanceWithObservers(
		benchmarkInstanceMachine,
		instanceIdle,
		func(context.Context, statemachine.Observation[instanceState, instanceEvent], *instanceData) {},
	)
	data := &instanceData{}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = instance.Fire(context.Background(), instanceStart, data)
		_, _ = instance.Fire(context.Background(), instanceFinish, data)
	}
}
