package queued_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/queued"
)

type qState int
type qEvent int

// qInstruction programs one event execution inside a cascade: it may fail the
// effect, enqueue follow-ups, cancel the root, or attempt a forbidden
// reentrant Fire.
type qInstruction struct {
	fail      bool
	enqueue   []qEvent
	cancel    bool
	reentrant bool
}

// qProgram is the shared data value; the effect pops one instruction per
// executed event, which lets an independent simulation predict the cascade.
type qProgram struct {
	mu           sync.Mutex
	instructions []qInstruction
	cursor       int
	executed     int
	trace        []qEvent
	violations   []string
	cancelRoot   context.CancelFunc
	runtime      *queued.Runtime[qState, qEvent, *qProgram]
}

func (p *qProgram) next(event qEvent) qInstruction {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.executed++
	p.trace = append(p.trace, event)
	if p.cursor >= len(p.instructions) {
		return qInstruction{}
	}
	instruction := p.instructions[p.cursor]
	p.cursor++
	return instruction
}

var errQueuedEffect = errors.New("fuzz: effect failed")

// qTable is total: every state accepts every event, so the model is exercised
// purely on cascade semantics.
func qTable() *statemachine.Machine[qState, qEvent, *qProgram] {
	var table []statemachine.Transition[qState, qEvent, *qProgram]
	for state := range 3 {
		for event := range 3 {
			from, ev := qState(state), qEvent(event)
			table = append(table, statemachine.Transition[qState, qEvent, *qProgram]{
				From: from, Event: ev, To: qNext(from, ev),
				Do: func(ctx context.Context, program *qProgram) error {
					instruction := program.next(ev)
					if instruction.reentrant {
						if _, err := program.runtime.Fire(ctx, ev, program); !errors.Is(err, queued.ErrReentrant) {
							program.mu.Lock()
							program.violations = append(program.violations, "reentrant fire not refused")
							program.mu.Unlock()
						}
					}
					if instruction.cancel && program.cancelRoot != nil {
						program.cancelRoot()
					}
					for _, follow := range instruction.enqueue {
						if err := program.runtime.Enqueue(ctx, follow, program); err != nil &&
							!errors.Is(err, queued.ErrRunLimit) {
							program.mu.Lock()
							program.violations = append(program.violations, "enqueue: "+err.Error())
							program.mu.Unlock()
						}
					}
					if instruction.fail {
						return errQueuedEffect
					}
					return nil
				},
			})
		}
	}
	return statemachine.MustCompile(table)
}

func qNext(state qState, event qEvent) qState {
	return qState((int(state) + 1 + int(event)) % 3)
}

// decodeInstructions maps bytes to instructions, two bytes each.
func decodeInstructions(data []byte) []qInstruction {
	count := min(len(data)/2, 24)
	instructions := make([]qInstruction, 0, count)
	for index := range count {
		control, extra := data[index*2], data[index*2+1]
		instruction := qInstruction{
			fail:      control&1 != 0,
			cancel:    control&2 != 0,
			reentrant: control&4 != 0,
		}
		for follow := range int(extra % 4) {
			instruction.enqueue = append(instruction.enqueue, qEvent((int(extra)+follow)%3))
		}
		instructions = append(instructions, instruction)
	}
	return instructions
}

// FuzzQueuedRuntimeModel runs byte-programmed cascades sequentially and
// checks the committed state after every root against an independent
// simulation of FIFO cascade semantics: follow-ups run after the root event,
// the cascade aborts on the first error or observed cancellation, and
// discarded follow-ups never run. Exact event traces, callback counts, and
// instruction cursors are checked without copying runtime progress into the
// model. Cancellation is synchronous inside an effect, so that effect commits
// on success and a queued successor observes cancellation before its callback.
func FuzzQueuedRuntimeModel(f *testing.F) {
	f.Add([]byte{0, 0}, []byte{0})
	f.Add([]byte{0, 3, 0, 0, 0, 0, 0, 0}, []byte{0})
	f.Add([]byte{0, 3, 0, 0, 0, 0}, []byte{0, 1})
	f.Add([]byte{1, 0, 0, 2}, []byte{2, 0})
	f.Add([]byte{4, 1, 2, 2, 0, 0}, []byte{1})
	f.Add([]byte{2, 3, 0, 0}, []byte{0, 2})
	f.Fuzz(func(t *testing.T, instructionBytes, roots []byte) {
		if len(roots) > 8 {
			roots = roots[:8]
		}
		program := &qProgram{instructions: decodeInstructions(instructionBytes)}
		machine := qTable()
		program.runtime = queued.New(machine, qState(0))

		// Independent simulation over the same instruction stream.
		simState := qState(0)
		simCursor := 0
		simExecuted := 0
		var simTrace []qEvent
		nextInstruction := func() qInstruction {
			if simCursor >= len(program.instructions) {
				return qInstruction{}
			}
			instruction := program.instructions[simCursor]
			simCursor++
			return instruction
		}

		for _, rootByte := range roots {
			rootEvent := qEvent(rootByte % 3)
			rootCtx, cancel := context.WithCancel(context.Background())
			program.mu.Lock()
			program.cancelRoot = cancel
			program.mu.Unlock()

			// Simulate the cascade this root should produce.
			wantErr := false
			canceled := false
			pending := []qEvent{rootEvent}
			accepted := 1
			for len(pending) != 0 {
				if canceled {
					break
				}
				event := pending[0]
				pending = pending[1:]
				simExecuted++
				simTrace = append(simTrace, event)
				instruction := nextInstruction()
				if instruction.cancel {
					canceled = true // observed before the next event begins
				}
				enqueued := instruction.enqueue[:min(len(instruction.enqueue), queued.DefaultMaxRunEvents-accepted)]
				accepted += len(enqueued)
				if instruction.fail {
					wantErr = true
					break
				}
				simState = qNext(simState, event)
				pending = append(pending, enqueued...)
			}

			state, err := program.runtime.Fire(rootCtx, rootEvent, program)
			cancel()
			if state != simState || program.runtime.State() != simState {
				t.Fatalf("state = %v/%v, model %v (root %v, instructions %+v)",
					state, program.runtime.State(), simState, rootEvent, program.instructions)
			}
			switch {
			case wantErr:
				if !errors.Is(err, errQueuedEffect) {
					t.Fatalf("cascade error = %v, want effect failure", err)
				}
			case canceled && len(pending) != 0:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled cascade error = %v, want context.Canceled", err)
				}
			default:
				if err != nil {
					t.Fatalf("cascade = %v", err)
				}
			}
			program.mu.Lock()
			trace := slices.Clone(program.trace)
			executed, cursor := program.executed, program.cursor
			violations := slices.Clone(program.violations)
			program.mu.Unlock()
			if !slices.Equal(trace, simTrace) || executed != simExecuted || cursor != simCursor {
				t.Fatalf("execution trace=%v count=%d cursor=%d; model trace=%v count=%d cursor=%d",
					trace, executed, cursor, simTrace, simExecuted, simCursor)
			}
			if len(violations) != 0 {
				t.Fatalf("violations: %v", violations)
			}
		}
	})
}

// FuzzQueuedRuntimeLimits exercises explicit root and run-event limits under
// concurrency. Every Fire must return, the sentinels must be the documented
// ones, and the Runtime must drain to an idle Status.
func FuzzQueuedRuntimeLimits(f *testing.F) {
	f.Add(uint8(1), uint8(1), uint8(4))
	f.Add(uint8(2), uint8(3), uint8(6))
	f.Add(uint8(4), uint8(6), uint8(2))
	f.Fuzz(func(t *testing.T, maxRoots, maxRunEvents, extra uint8) {
		limits := queued.Limits{
			MaxRoots:     int(maxRoots%4) + 1,
			MaxRunEvents: int(maxRunEvents%6) + 1,
		}
		overflow := int(extra%4) + limits.MaxRunEvents + 1

		var runLimitSeen bool
		var mu sync.Mutex
		table := []statemachine.Transition[qState, qEvent, *qProgram]{
			{
				From: 0, Event: 0, To: 1,
				Do: func(ctx context.Context, program *qProgram) error {
					for range overflow {
						if err := program.runtime.Enqueue(ctx, 1, program); errors.Is(err, queued.ErrRunLimit) {
							mu.Lock()
							runLimitSeen = true
							mu.Unlock()
						}
					}
					return nil
				},
			},
			{From: 1, Event: 1, To: 1},
			{From: 0, Event: 1, To: 0},
			{From: 1, Event: 0, To: 1},
		}
		machine := statemachine.MustCompile(table)
		runtime_, err := queued.NewWithLimits(machine, qState(0), limits)
		if err != nil {
			t.Fatal(err)
		}
		program := &qProgram{runtime: runtime_}

		attempts := limits.MaxRoots + 3
		var wg sync.WaitGroup
		results := make([]error, attempts)
		for index := range attempts {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				_, results[index] = runtime_.Fire(context.Background(), 0, program)
			}(index)
		}
		wg.Wait()

		for _, err := range results {
			if err != nil && !errors.Is(err, queued.ErrRootLimit) {
				t.Fatalf("root fire = %v", err)
			}
		}
		// The run-event cap counts the root itself, so a cap of one admits no
		// follow-up at all and the overflow enqueue must always trip it.
		mu.Lock()
		if !runLimitSeen {
			mu.Unlock()
			t.Fatalf("enqueue overflow of %d never returned ErrRunLimit (cap %d)", overflow, limits.MaxRunEvents)
		}
		mu.Unlock()

		deadline := time.Now().Add(time.Second)
		for {
			status := runtime_.Status()
			if !status.Running && status.OutstandingRoots == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("runtime did not drain: %+v", status)
			}
			runtime.Gosched()
		}
	})
}

// FuzzQueuedCancellation checks that a root canceled before admission never
// runs application code and that later roots are unaffected.
func FuzzQueuedCancellation(f *testing.F) {
	f.Add(uint8(0))
	f.Add(uint8(2))
	f.Fuzz(func(t *testing.T, eventByte uint8) {
		calls := 0
		table := []statemachine.Transition[qState, qEvent, *qProgram]{}
		for state := range 3 {
			for event := range 3 {
				from, ev := qState(state), qEvent(event)
				table = append(table, statemachine.Transition[qState, qEvent, *qProgram]{
					From: from, Event: ev, To: qNext(from, ev),
					Do: func(context.Context, *qProgram) error {
						calls++
						return nil
					},
				})
			}
		}
		runtime_ := queued.New(statemachine.MustCompile(table), qState(0))
		program := &qProgram{runtime: runtime_}

		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		state, err := runtime_.Fire(canceled, qEvent(eventByte%3), program)
		if !errors.Is(err, context.Canceled) || calls != 0 || state != qState(0) {
			t.Fatalf("pre-canceled root = %v, %v, calls %d", state, err, calls)
		}
		if _, err := runtime_.Fire(context.Background(), qEvent(eventByte%3), program); err != nil || calls != 1 {
			t.Fatalf("later root = %v, calls %d", err, calls)
		}
	})
}
