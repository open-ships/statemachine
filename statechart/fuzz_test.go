package statechart_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/open-ships/statemachine"
	"github.com/open-ships/statemachine/statechart"
)

type scState int
type scEvent int

// scModel is an independent reference implementation of the statechart
// semantics: parent forest, initial-state descent, ancestor-walking
// selection, and LCA-based exit/entry paths. The fuzz targets assert that the
// library and this model agree on every observable outcome.
type scModel struct {
	stateCount int
	parent     map[scState]scState
	initial    map[scState]scState
	rows       []scRow
}

type scRow struct {
	from     scState
	event    scEvent
	to       scState
	kind     statechart.Kind
	guarded  bool
	declines bool
}

func (m *scModel) destination(state scState) scState {
	for {
		next, ok := m.initial[state]
		if !ok {
			return state
		}
		state = next
	}
}

func (m *scModel) ancestors(state scState) []scState {
	path := []scState{state}
	for {
		parent, ok := m.parent[state]
		if !ok {
			return path
		}
		path = append(path, parent)
		state = parent
	}
}

// select walks from the active state toward the root, trying rows in
// declaration order, exactly as Chart.selectTransition documents.
func (m *scModel) selectRow(source scState, event scEvent) (scRow, scState, int, bool) {
	declined := 0
	for state := source; ; {
		for _, row := range m.rows {
			if row.from != state || row.event != event {
				continue
			}
			if row.guarded && row.declines {
				declined++
				continue
			}
			return row, state, declined, true
		}
		parent, ok := m.parent[state]
		if !ok {
			return scRow{}, 0, declined, false
		}
		state = parent
	}
}

func (m *scModel) externalPaths(source, target, destination scState) (exits, entries []scState) {
	inTarget := make(map[scState]bool)
	for _, state := range m.ancestors(target) {
		inTarget[state] = true
	}
	var lca scState
	common := false
	for _, state := range m.ancestors(source) {
		if inTarget[state] {
			lca, common = state, true
			break
		}
	}
	for _, state := range m.ancestors(source) {
		if common && state == lca {
			break
		}
		exits = append(exits, state)
	}
	up := m.ancestors(destination)
	for _, state := range up {
		if common && state == lca {
			break
		}
		entries = append(entries, state)
	}
	for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
		entries[left], entries[right] = entries[right], entries[left]
	}
	return exits, entries
}

func (m *scModel) reentryPaths(source, handler, destination scState) (exits, entries []scState) {
	for state := source; ; state = m.parent[state] {
		exits = append(exits, state)
		if state == handler {
			break
		}
	}
	for state := destination; ; state = m.parent[state] {
		entries = append(entries, state)
		if state == handler {
			break
		}
	}
	for left, right := 0, len(entries)-1; left < right; left, right = left+1, right-1 {
		entries[left], entries[right] = entries[right], entries[left]
	}
	return exits, entries
}

var errModelDecline = errors.New("fuzz: guard declined")

type lifecycleStep struct {
	state scState
	move  string
}

// decodeModel builds a guaranteed-valid randomized hierarchy: parents always
// point at lower indices, initials are immediate children, and transition
// kinds respect the compile rules.
func decodeModel(shape []byte) (*scModel, statechart.Definition[scState, scEvent, *[]lifecycleStep], bool) {
	if len(shape) < 3 {
		return nil, statechart.Definition[scState, scEvent, *[]lifecycleStep]{}, false
	}
	model := &scModel{
		stateCount: int(shape[0]%7) + 2,
		parent:     make(map[scState]scState),
		initial:    make(map[scState]scState),
	}
	var definition statechart.Definition[scState, scEvent, *[]lifecycleStep]
	for index := range model.stateCount {
		state := scState(index)
		definition.States = append(definition.States, statechart.State[scState, scEvent, *[]lifecycleStep]{
			Name: state,
			Entry: []statechart.Action[scState, scEvent, *[]lifecycleStep]{
				func(_ context.Context, _ statechart.Info[scState, scEvent], trace *[]lifecycleStep) error {
					*trace = append(*trace, lifecycleStep{state: state, move: "entry"})
					return nil
				},
			},
			Exit: []statechart.Action[scState, scEvent, *[]lifecycleStep]{
				func(_ context.Context, _ statechart.Info[scState, scEvent], trace *[]lifecycleStep) error {
					*trace = append(*trace, lifecycleStep{state: state, move: "exit"})
					return nil
				},
			},
		})
	}
	// Parents: each state after the root may attach to any lower index, which
	// makes cycles unrepresentable by construction.
	cursor := 1
	for index := 1; index < model.stateCount && cursor < len(shape); index++ {
		choice := shape[cursor]
		cursor++
		if choice%2 == 0 {
			continue
		}
		parent := scState(int(choice/2) % index)
		model.parent[scState(index)] = parent
		definition.Substates = append(definition.Substates, statechart.Substate[scState]{
			Child: scState(index), Parent: parent,
		})
	}
	// Initials: a parent may name one of its immediate children.
	children := make(map[scState][]scState)
	for child, parent := range model.parent {
		children[parent] = append(children[parent], child)
	}
	for parent := scState(0); int(parent) < model.stateCount && cursor < len(shape); parent++ {
		choice := shape[cursor]
		cursor++
		direct := children[parent]
		if len(direct) == 0 || choice%2 == 0 {
			continue
		}
		// Pick deterministically: the smallest child index keeps the model and
		// the definition trivially in agreement.
		smallest := direct[0]
		for _, candidate := range direct {
			if candidate < smallest {
				smallest = candidate
			}
		}
		model.initial[parent] = smallest
		definition.Initials = append(definition.Initials, statechart.Initial[scState]{
			Parent: parent, Child: smallest,
		})
	}
	// Transitions: kinds respect the compile rules by construction.
	for row := 0; cursor+3 < len(shape) && row < 12; row++ {
		chunk := shape[cursor : cursor+4]
		cursor += 4
		from := scState(int(chunk[0]) % model.stateCount)
		event := scEvent(chunk[1] % 3)
		kind := statechart.Kind(chunk[2] % 3)
		to := from
		if kind == statechart.External {
			if model.stateCount < 2 {
				continue
			}
			to = scState((int(from) + 1 + int(chunk[2]/3)%(model.stateCount-1)) % model.stateCount)
		}
		guarded := chunk[3]&1 != 0
		declines := chunk[3]&2 != 0
		modelRow := scRow{from: from, event: event, to: to, kind: kind, guarded: guarded, declines: declines}
		transition := statechart.Transition[scState, scEvent, *[]lifecycleStep]{
			From: from, Event: event, To: to, Kind: kind,
		}
		if guarded {
			transition.Guard = func(context.Context, statechart.Info[scState, scEvent], *[]lifecycleStep) error {
				if declines {
					return errModelDecline
				}
				return nil
			}
		} else if declines {
			// An unguarded row cannot decline; keep model and chart aligned.
			modelRow.declines = false
		}
		// Compile rejects rows hidden by an earlier unguarded row with the
		// same From and Event; the model skips them for the same reason.
		hidden := false
		for _, existing := range model.rows {
			if existing.from == from && existing.event == event && !existing.guarded {
				hidden = true
				break
			}
		}
		if hidden {
			continue
		}
		model.rows = append(model.rows, modelRow)
		definition.Transitions = append(definition.Transitions, transition)
	}
	// Declaration order is independent of hierarchy order. Reversing these
	// inputs exercises children declared before parents and initials whose
	// destination has not yet appeared in the state slice.
	if shape[0]&0x80 != 0 {
		slices.Reverse(definition.States)
		slices.Reverse(definition.Substates)
		slices.Reverse(definition.Initials)
	}
	return model, definition, true
}

// Membership is derived directly from the declared parent relationships,
// independently of the compiled intervals and transition lifecycle paths.
func assertModelPosition(t *testing.T, model *scModel, position statechart.Position[scState], active scState) {
	t.Helper()
	if got, ok := position.Active(); !ok || got != active {
		t.Fatalf("Position.Active = %v/%v, want %v", got, ok, active)
	}
	lineage := model.ancestors(active)
	if got := slices.Collect(position.Path()); !slices.Equal(got, lineage) {
		t.Fatalf("Position.Path = %v, want %v", got, lineage)
	}
	for state := scState(0); int(state) < model.stateCount; state++ {
		want := statechart.StatusInactive
		if state == active {
			want = statechart.StatusActive
		} else if slices.Contains(lineage, state) {
			want = statechart.StatusEnclosing
		}
		if got, known := position.Status(state); !known || got != want {
			t.Fatalf("Position.Status(%v) = %v/%v, want %v", state, got, known, want)
		}
	}
}

func assertObservationMembership(t *testing.T, model *scModel, from, to scState, observations []statechart.Observation[scState, scEvent]) {
	t.Helper()
	membership := make(map[scState]int)
	for _, state := range model.ancestors(from) {
		membership[state] = 1
	}
	for _, observation := range observations {
		switch observation.Move {
		case statemachine.Exited:
			if membership[observation.State] != 1 {
				t.Fatalf("exited inactive state %v", observation.State)
			}
			membership[observation.State]--
		case statemachine.Entered:
			if membership[observation.State] != 0 {
				t.Fatalf("entered already active state %v", observation.State)
			}
			membership[observation.State]++
		default:
			t.Fatalf("invalid observation Move %v", observation.Move)
		}
	}
	lineage := model.ancestors(to)
	for state := scState(0); int(state) < model.stateCount; state++ {
		want := 0
		if slices.Contains(lineage, state) {
			want = 1
		}
		if membership[state] != want {
			t.Fatalf("observation deltas left membership[%v] = %d, want %d", state, membership[state], want)
		}
	}
}

// FuzzStatechartCompile feeds arbitrary — including invalid — definitions to
// Compile, which must never panic; valid results must create an Instance at
// every declared state with the initial chain resolved.
func FuzzStatechartCompile(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7}, uint8(3))
	f.Add([]byte{1, 1, 1, 1, 1, 1}, uint8(0))
	f.Add([]byte{7, 0, 0, 2, 2, 9, 9, 9, 1}, uint8(9))
	f.Fuzz(func(t *testing.T, shape []byte, hostile uint8) {
		if len(shape) == 0 {
			return
		}
		if len(shape) > 64 {
			shape = shape[:64]
		}
		var definition statechart.Definition[scState, scEvent, *[]lifecycleStep]
		stateCount := int(hostile%6) + 1
		for index := range stateCount {
			// Deliberately allow duplicates when hostile is odd.
			name := scState(index)
			if hostile%2 == 1 && index > 0 {
				name = scState(int(shape[index%len(shape)]) % stateCount)
			}
			definition.States = append(definition.States, statechart.State[scState, scEvent, *[]lifecycleStep]{Name: name})
		}
		for index := 0; index+1 < len(shape); index += 2 {
			definition.Substates = append(definition.Substates, statechart.Substate[scState]{
				Child:  scState(int(shape[index]) % (stateCount + 2)), // may be undeclared
				Parent: scState(int(shape[index+1]) % (stateCount + 2)),
			})
			definition.Initials = append(definition.Initials, statechart.Initial[scState]{
				Parent: scState(int(shape[index+1]) % (stateCount + 2)),
				Child:  scState(int(shape[index]) % (stateCount + 2)),
			})
			definition.Transitions = append(definition.Transitions, statechart.Transition[scState, scEvent, *[]lifecycleStep]{
				From:  scState(int(shape[index]) % (stateCount + 2)),
				Event: scEvent(shape[index+1] % 3),
				To:    scState(int(shape[index+1]) % (stateCount + 2)),
				Kind:  statechart.Kind(shape[index] % 5), // may be invalid
			})
		}
		chart, err := statechart.Compile(definition)
		if err != nil {
			if err.Error() == "" {
				t.Fatal("empty compile error")
			}
			return
		}
		for _, state := range definition.States {
			instance, err := chart.New(state.Name)
			if err != nil || instance == nil {
				t.Fatalf("New(%v) = %v", state.Name, err)
			}
			_ = instance.State()
		}
	})
}

// FuzzStatechartFire builds valid randomized hierarchies and checks every
// Fire outcome — committed state, refusal identity, lifecycle trace, and the
// committed observation batch — against the reference model.
func FuzzStatechartFire(f *testing.F) {
	f.Add([]byte{2, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0}, []byte{0, 1, 2})
	f.Add([]byte{5, 1, 3, 1, 1, 1, 2, 0, 0, 0, 3, 1, 1, 3, 0, 2, 0, 0}, []byte{0, 0, 1, 2, 0})
	f.Add([]byte{3, 1, 1, 0, 0, 1, 2, 2, 0, 1, 0, 1, 2, 1}, []byte{1, 0, 2, 1})
	f.Add([]byte{6, 1, 3, 5, 1, 1, 1, 0, 0, 0, 1, 1, 2, 3, 2, 1, 0, 2, 4, 0, 1, 0}, []byte{0, 1, 2, 0, 1})
	f.Fuzz(func(t *testing.T, shape, events []byte) {
		if len(shape) > 64 {
			shape = shape[:64]
		}
		if len(events) > 32 {
			events = events[:32]
		}
		model, definition, ok := decodeModel(shape)
		if !ok {
			return
		}
		chart, err := statechart.Compile(definition)
		if err != nil {
			t.Fatalf("valid-by-construction definition rejected: %v\n%+v", err, definition)
		}

		var observations []statechart.Observation[scState, scEvent]
		instance, err := chart.NewWithObservers(0, func(
			_ context.Context, observation statechart.Observation[scState, scEvent], _ *[]lifecycleStep,
		) {
			observations = append(observations, observation)
		})
		if err != nil {
			t.Fatal(err)
		}
		active := model.destination(0)
		if instance.State() != active {
			t.Fatalf("initial state = %v, model %v", instance.State(), active)
		}

		var nextSeq uint64 = 1
		for _, eventByte := range events {
			event := scEvent(eventByte % 3)
			trace := &[]lifecycleStep{}
			observations = observations[:0]
			before, _ := instance.Position()
			assertModelPosition(t, model, before, active)
			state, err := instance.Fire(context.Background(), event, trace)
			assertModelPosition(t, model, before, active) // saved snapshots stay valid

			row, handler, declined, selected := model.selectRow(active, event)
			if !selected {
				if !errors.Is(err, statechart.ErrNotPermitted) || state != active {
					t.Fatalf("refusal = %v, %v (model active %v)", state, err, active)
				}
				if declined > 0 && !errors.Is(err, errModelDecline) {
					t.Fatalf("refusal lost guard reason: %v", err)
				}
				if len(*trace) != 0 || len(observations) != 0 {
					t.Fatalf("refusal ran lifecycle work: %+v / %+v", *trace, observations)
				}
				continue
			}
			if err != nil {
				t.Fatalf("Fire(%v) in %v = %v (row %+v)", event, active, err, row)
			}

			var exits, entries []scState
			destination := active
			switch row.kind {
			case statechart.Internal:
				// No lifecycle work and no state change.
			case statechart.Reentry:
				destination = model.destination(row.to)
				exits, entries = model.reentryPaths(active, handler, destination)
			default:
				destination = model.destination(row.to)
				exits, entries = model.externalPaths(active, row.to, destination)
			}
			if state != destination || instance.State() != destination {
				t.Fatalf("Fire(%v) state = %v/%v, model %v (row %+v)", event, state, instance.State(), destination, row)
			}

			var wantTrace []lifecycleStep
			for _, exit := range exits {
				wantTrace = append(wantTrace, lifecycleStep{state: exit, move: "exit"})
			}
			for _, entry := range entries {
				wantTrace = append(wantTrace, lifecycleStep{state: entry, move: "entry"})
			}
			if fmt.Sprint(*trace) != fmt.Sprint(wantTrace) {
				t.Fatalf("trace = %v, model %v (row %+v, active %v)", *trace, wantTrace, row, active)
			}

			total := len(exits) + len(entries)
			if len(observations) != total {
				t.Fatalf("observations = %d, want %d", len(observations), total)
			}
			for index, observation := range observations {
				wantMove := statemachine.Exited
				wantState := scState(0)
				if index < len(exits) {
					wantState = exits[index]
				} else {
					wantMove = statemachine.Entered
					wantState = entries[index-len(exits)]
				}
				if observation.Seq != nextSeq+uint64(index) || observation.Step != nextSeq ||
					observation.Run != nextSeq || observation.Remaining != uint64(total-index-1) ||
					observation.Move != wantMove || observation.State != wantState ||
					observation.Event != event || !observation.At.Equal(observations[0].At) {
					t.Fatalf("observation[%d] = %+v, want seq %d step %d %v %v",
						index, observation, nextSeq+uint64(index), nextSeq, wantMove, wantState)
				}
				if observation.Info.Destination != destination || observation.Info.Source != active {
					t.Fatalf("observation info = %+v", observation.Info)
				}
			}
			if total != 0 {
				nextSeq += uint64(total)
			}
			assertObservationMembership(t, model, active, destination, observations)
			after, _ := instance.Position()
			assertModelPosition(t, model, after, destination)
			active = destination
		}
	})
}

// Failures are injected at an arbitrary callback in each event. Successful
// callbacks may have effects, but failure must preserve Position and emit no
// observations, and ActionError must identify only completed nonnil callbacks.
func FuzzStatechartFailureCommit(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 1}, uint8(0))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 1}, uint8(1))
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 1}, uint8(2))
	f.Add([]byte{130, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0, 0}, []byte{0, 1, 2}, uint8(3))
	f.Fuzz(func(t *testing.T, shape, events []byte, failureByte uint8) {
		shape = shape[:min(len(shape), 64)]
		events = events[:min(len(events), 32)]
		model, definition, ok := decodeModel(shape)
		if !ok {
			return
		}
		sentinel := errors.New("injected lifecycle failure")
		calls, failureAt := 0, int(failureByte%16)
		completed := map[statechart.Phase]int{}
		var expected *statechart.ActionError
		wrap := func(original statechart.Action[scState, scEvent, *[]lifecycleStep], phase statechart.Phase, state any, index int) statechart.Action[scState, scEvent, *[]lifecycleStep] {
			return func(ctx context.Context, info statechart.Info[scState, scEvent], trace *[]lifecycleStep) error {
				call := calls
				calls++
				if call == failureAt {
					expected = &statechart.ActionError{Phase: phase, State: state, ActionIndex: index, Completed: completed[phase], Err: sentinel}
					return sentinel
				}
				if original != nil {
					if err := original(ctx, info, trace); err != nil {
						return err
					}
				}
				completed[phase]++
				return nil
			}
		}
		for index := range definition.States {
			node := &definition.States[index]
			node.Entry = []statechart.Action[scState, scEvent, *[]lifecycleStep]{nil, wrap(node.Entry[0], statechart.PhaseEntry, node.Name, 1), nil}
			node.Exit = []statechart.Action[scState, scEvent, *[]lifecycleStep]{nil, wrap(node.Exit[0], statechart.PhaseExit, node.Name, 1), nil}
		}
		for index := range definition.Transitions {
			definition.Transitions[index].Do = wrap(nil, statechart.PhaseEffect, nil, 0)
		}
		chart, err := statechart.Compile(definition)
		if err != nil {
			t.Fatal(err)
		}
		var observations []statechart.Observation[scState, scEvent]
		instance, err := chart.NewWithObservers(scState(failureByte%uint8(model.stateCount)), func(_ context.Context, observation statechart.Observation[scState, scEvent], _ *[]lifecycleStep) {
			observations = append(observations, observation)
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, eventByte := range events {
			calls = 0
			clear(completed)
			expected = nil
			observations = observations[:0]
			source := instance.State()
			before, _ := instance.Position()
			trace := &[]lifecycleStep{}
			destination, err := instance.Fire(context.Background(), scEvent(eventByte%3), trace)
			assertModelPosition(t, model, before, source)
			if expected != nil {
				var actual *statechart.ActionError
				if !errors.As(err, &actual) || !errors.Is(err, sentinel) {
					t.Fatalf("failure result: %v", err)
				}
				if actual.Phase != expected.Phase || actual.State != expected.State || actual.ActionIndex != expected.ActionIndex || actual.Completed != expected.Completed || actual.Committed {
					t.Fatalf("ActionError = %+v, want %+v", actual, expected)
				}
				if destination != source || len(observations) != 0 {
					t.Fatalf("failed callback committed %v -> %v / %+v", source, destination, observations)
				}
			} else if err != nil && !errors.Is(err, statechart.ErrNotPermitted) {
				t.Fatal(err)
			}
			after, _ := instance.Position()
			assertModelPosition(t, model, after, destination)
			assertObservationMembership(t, model, source, destination, observations)
		}
	})
}
