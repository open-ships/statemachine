package supervised

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"strings"
)

// Attempt identifies one event accepted by a Supervisor. Attempt is assigned
// before preconditions run, so refusals and faults retain the same identifier
// the caller received.
type Attempt[S, E comparable] struct {
	DefinitionID string
	ID           uint64
	Revision     uint64
	From         S
	Event        E
}

// Change identifies the transition selected for an Attempt.
type Change[S, E comparable] struct {
	Attempt[S, E]
	TransitionID string
	To           S
}

// Precondition is a mandatory, non-routing check performed before guarded row
// selection. Unlike a Guard, a failed Precondition latches a Fault and can
// never fall through to another transition.
type Precondition[S, E comparable, T any] func(context.Context, Attempt[S, E], T) error

// Check is a mandatory check for one selected Change. Checks are used for
// transition preconditions, invariants, physical verification, and
// postconditions. A failed Check latches a Fault.
type Check[S, E comparable, T any] func(context.Context, Change[S, E], T) error

// Guard routes an Attempt among rows with the same From and Event. Nil applies;
// an error declines this row and tries the next one.
type Guard[S, E comparable, T any] func(context.Context, Change[S, E], T) error

// Action issues one selected Change to an external system. Returning nil means
// only that Issue completed; it is not physical verification.
type Action[S, E comparable, T any] func(context.Context, Change[S, E], T) error

// Reconciler verifies that a restored or faulted Supervisor's committed state
// agrees with application-owned physical and durable state. It must not issue
// motion. Every Reconciler must pass before Start or Recover succeeds.
type Reconciler[S comparable, T any] func(context.Context, Snapshot[S], T) error

// State declares one state in a strict Definition. A terminal State implicitly
// refuses every declared event. A non-terminal State must either have at least
// one transition or explicitly Refuse each declared event.
type State[S, E comparable] struct {
	Name     S
	Terminal bool
	Refuse   []E
}

// Transition declares one supervised row. ID must be stable and unique.
//
// A purely logical transition leaves Issue and Verify nil and commits during
// Supervisor.Issue. Otherwise Issue and Verify must either both be present or
// both be absent. Verify receives fresh data from Supervisor.Verify and must
// establish the application-specific physical completion condition.
type Transition[S, E comparable, T any] struct {
	ID            string
	From          S
	Event         E
	To            S
	Guard         Guard[S, E, T]
	Preconditions []Check[S, E, T]
	Issue         Action[S, E, T]
	Verify        Check[S, E, T]
}

// Definition is the strict input to Compile.
//
// Preconditions run before routing. Invariants run after selection and again
// after Verify. Postconditions run immediately before logical commit.
// Reconcile runs before a new or restored Supervisor starts and before a
// faulted Supervisor recovers. All four global check groups must be non-empty;
// use an explicit function returning nil when a phase is intentionally empty.
type Definition[S, E comparable, T any] struct {
	ID             string
	States         []State[S, E]
	Events         []E
	Initial        S
	Transitions    []Transition[S, E, T]
	Preconditions  []Precondition[S, E, T]
	Invariants     []Check[S, E, T]
	Postconditions []Check[S, E, T]
	Reconcile      []Reconciler[S, T]
}

type transitionKey[S, E comparable] struct {
	state S
	event E
}

type compiledTransition[S, E comparable, T any] struct {
	id            string
	from          S
	event         E
	to            S
	guard         Guard[S, E, T]
	preconditions []Check[S, E, T]
	issue         Action[S, E, T]
	verify        Check[S, E, T]
}

// Machine is an immutable strict flat definition shared by Supervisors. It
// retains structural facts for inspection but never owns execution state.
type Machine[S, E comparable, T any] struct {
	id             string
	initial        S
	states         []StateInfo[S]
	events         []E
	stateIndex     map[S]StateInfo[S]
	eventSet       map[E]struct{}
	refused        map[transitionKey[S, E]]struct{}
	transitions    []TransitionInfo[S, E]
	rows           map[transitionKey[S, E]][]compiledTransition[S, E, T]
	preconditions  []Precondition[S, E, T]
	invariants     []Check[S, E, T]
	postconditions []Check[S, E, T]
	reconcile      []Reconciler[S, T]
}

// StateInfo is one immutable state declaration exposed for inspection.
type StateInfo[S comparable] struct {
	Name     S
	Terminal bool
}

// TransitionInfo is one immutable transition declaration exposed for
// inspection. It contains no callback values.
type TransitionInfo[S, E comparable] struct {
	ID                string
	From              S
	Event             E
	To                S
	Guarded           bool
	PreconditionCount int
	Issues            bool
}

// Compile validates and copies a strict Definition. It reports all
// independently discoverable defects together with errors.Join.
func Compile[S, E comparable, T any](definition Definition[S, E, T]) (*Machine[S, E, T], error) {
	var problems []error
	if strings.TrimSpace(definition.ID) == "" {
		problems = append(problems, errors.New("supervised: definition ID is empty"))
	}
	if reflect.TypeFor[S]().Kind() == reflect.Interface {
		problems = append(problems, errors.New("supervised: state type must not be an interface"))
	}
	if reflect.TypeFor[E]().Kind() == reflect.Interface {
		problems = append(problems, errors.New("supervised: event type must not be an interface"))
	}
	if len(definition.States) == 0 {
		problems = append(problems, errors.New("supervised: definition has no states"))
	}
	if len(definition.Events) == 0 {
		problems = append(problems, errors.New("supervised: definition has no events"))
	}
	checkRequiredCallbacks("preconditions", len(definition.Preconditions), &problems)
	for index, callback := range definition.Preconditions {
		if callback == nil {
			problems = append(problems, fmt.Errorf("supervised: preconditions[%d] is nil", index))
		}
	}
	checkRequiredCallbacks("invariants", len(definition.Invariants), &problems)
	for index, callback := range definition.Invariants {
		if callback == nil {
			problems = append(problems, fmt.Errorf("supervised: invariants[%d] is nil", index))
		}
	}
	checkRequiredCallbacks("postconditions", len(definition.Postconditions), &problems)
	for index, callback := range definition.Postconditions {
		if callback == nil {
			problems = append(problems, fmt.Errorf("supervised: postconditions[%d] is nil", index))
		}
	}
	checkRequiredCallbacks("reconcile", len(definition.Reconcile), &problems)
	for index, callback := range definition.Reconcile {
		if callback == nil {
			problems = append(problems, fmt.Errorf("supervised: reconcile[%d] is nil", index))
		}
	}

	stateAt := make(map[S]int, len(definition.States))
	states := make([]StateInfo[S], 0, len(definition.States))
	stateIndex := make(map[S]StateInfo[S], len(definition.States))
	refused := make(map[transitionKey[S, E]]struct{})
	for index, state := range definition.States {
		if previous, exists := stateAt[state.Name]; exists {
			problems = append(problems, fmt.Errorf(
				"supervised: states[%d] duplicates state %v declared at states[%d]",
				index, state.Name, previous,
			))
			continue
		}
		stateAt[state.Name] = index
		info := StateInfo[S]{Name: state.Name, Terminal: state.Terminal}
		states = append(states, info)
		stateIndex[state.Name] = info
	}
	if _, known := stateAt[definition.Initial]; !known && len(definition.States) != 0 {
		problems = append(problems, fmt.Errorf(
			"supervised: initial state %v is undeclared", definition.Initial,
		))
	}

	eventAt := make(map[E]int, len(definition.Events))
	events := make([]E, 0, len(definition.Events))
	for index, event := range definition.Events {
		if previous, exists := eventAt[event]; exists {
			problems = append(problems, fmt.Errorf(
				"supervised: events[%d] duplicates event %v declared at events[%d]",
				index, event, previous,
			))
			continue
		}
		eventAt[event] = index
		events = append(events, event)
	}
	for stateIndexInDefinition, state := range definition.States {
		seen := make(map[E]int, len(state.Refuse))
		for refusalIndex, event := range state.Refuse {
			if previous, duplicate := seen[event]; duplicate {
				problems = append(problems, fmt.Errorf(
					"supervised: states[%d].Refuse[%d] duplicates event %v at Refuse[%d]",
					stateIndexInDefinition, refusalIndex, event, previous,
				))
				continue
			}
			seen[event] = refusalIndex
			if _, known := eventAt[event]; !known {
				problems = append(problems, fmt.Errorf(
					"supervised: states[%d].Refuse[%d] names undeclared event %v",
					stateIndexInDefinition, refusalIndex, event,
				))
				continue
			}
			refused[transitionKey[S, E]{state.Name, event}] = struct{}{}
		}
	}

	rows := make(map[transitionKey[S, E]][]compiledTransition[S, E, T])
	transitionInfos := make([]TransitionInfo[S, E], 0, len(definition.Transitions))
	idAt := make(map[string]int, len(definition.Transitions))
	unguarded := make(map[transitionKey[S, E]]int, len(definition.Transitions))
	for index, transition := range definition.Transitions {
		if strings.TrimSpace(transition.ID) == "" {
			problems = append(problems, fmt.Errorf("supervised: transitions[%d] has an empty ID", index))
		} else if previous, duplicate := idAt[transition.ID]; duplicate {
			problems = append(problems, fmt.Errorf(
				"supervised: transitions[%d] duplicates ID %q declared at transitions[%d]",
				index, transition.ID, previous,
			))
		} else {
			idAt[transition.ID] = index
		}
		fromKnown := knownDefinitionValue(stateAt, transition.From, "transitions", index, "From", &problems)
		toKnown := knownDefinitionValue(stateAt, transition.To, "transitions", index, "To", &problems)
		eventKnown := knownDefinitionValue(eventAt, transition.Event, "transitions", index, "Event", &problems)
		if (transition.Issue == nil) != (transition.Verify == nil) {
			problems = append(problems, fmt.Errorf(
				"supervised: transitions[%d] must declare both Issue and Verify or neither", index,
			))
		}
		for checkIndex, check := range transition.Preconditions {
			if check == nil {
				problems = append(problems, fmt.Errorf(
					"supervised: transitions[%d].Preconditions[%d] is nil", index, checkIndex,
				))
			}
		}

		key := transitionKey[S, E]{transition.From, transition.Event}
		if previous, hidden := unguarded[key]; hidden {
			problems = append(problems, fmt.Errorf(
				"supervised: transitions[%d] (%v on %v) is unreachable: transitions[%d] has the same From and Event and no Guard",
				index, transition.From, transition.Event, previous,
			))
		}
		if transition.Guard == nil {
			if _, already := unguarded[key]; !already {
				unguarded[key] = index
			}
		}
		if _, explicitlyRefused := refused[key]; explicitlyRefused {
			problems = append(problems, fmt.Errorf(
				"supervised: transitions[%d] handles event %v explicitly refused by state %v",
				index, transition.Event, transition.From,
			))
		}
		if fromKnown {
			if stateIndex[transition.From].Terminal {
				problems = append(problems, fmt.Errorf(
					"supervised: transitions[%d] leaves terminal state %v", index, transition.From,
				))
			}
		}
		if fromKnown && toKnown && eventKnown {
			rows[key] = append(rows[key], compiledTransition[S, E, T]{
				id: transition.ID, from: transition.From, event: transition.Event, to: transition.To,
				guard:         transition.Guard,
				preconditions: append([]Check[S, E, T](nil), transition.Preconditions...),
				issue:         transition.Issue, verify: transition.Verify,
			})
		}
		transitionInfos = append(transitionInfos, TransitionInfo[S, E]{
			ID: transition.ID, From: transition.From, Event: transition.Event, To: transition.To,
			Guarded: transition.Guard != nil, PreconditionCount: len(transition.Preconditions),
			Issues: transition.Issue != nil,
		})
	}

	for _, state := range states {
		if state.Terminal {
			continue
		}
		hasOutgoing := false
		for _, event := range events {
			key := transitionKey[S, E]{state.Name, event}
			_, hasRows := rows[key]
			hasOutgoing = hasOutgoing || hasRows
			_, refuses := refused[key]
			if !hasRows && !refuses {
				problems = append(problems, fmt.Errorf(
					"supervised: non-terminal state %v neither handles nor refuses event %v",
					state.Name, event,
				))
			}
		}
		if !hasOutgoing {
			problems = append(problems, fmt.Errorf(
				"supervised: non-terminal state %v has no outgoing transition", state.Name,
			))
		}
	}

	if _, initialKnown := stateAt[definition.Initial]; initialKnown {
		reachable := map[S]bool{definition.Initial: true}
		for grew := true; grew; {
			grew = false
			for _, transition := range definition.Transitions {
				if reachable[transition.From] && !reachable[transition.To] {
					reachable[transition.To] = true
					grew = true
				}
			}
		}
		for _, state := range states {
			if !reachable[state.Name] {
				problems = append(problems, fmt.Errorf(
					"supervised: state %v is unreachable from initial state %v",
					state.Name, definition.Initial,
				))
			}
		}
	}

	if len(problems) != 0 {
		return nil, errors.Join(problems...)
	}
	return &Machine[S, E, T]{
		id: definition.ID, initial: definition.Initial,
		states: states, events: events, stateIndex: stateIndex, eventSet: copySet(eventAt),
		refused: refused, transitions: transitionInfos, rows: rows,
		preconditions:  append([]Precondition[S, E, T](nil), definition.Preconditions...),
		invariants:     append([]Check[S, E, T](nil), definition.Invariants...),
		postconditions: append([]Check[S, E, T](nil), definition.Postconditions...),
		reconcile:      append([]Reconciler[S, T](nil), definition.Reconcile...),
	}, nil
}

func checkRequiredCallbacks(name string, count int, problems *[]error) {
	if count == 0 {
		*problems = append(*problems, fmt.Errorf("supervised: definition has no %s", name))
	}
}

func knownDefinitionValue[V comparable](
	values map[V]int,
	value V,
	section string,
	index int,
	field string,
	problems *[]error,
) bool {
	if _, known := values[value]; known {
		return true
	}
	*problems = append(*problems, fmt.Errorf(
		"supervised: %s[%d].%s names undeclared value %v", section, index, field, value,
	))
	return false
}

func copySet[V comparable](values map[V]int) map[V]struct{} {
	result := make(map[V]struct{}, len(values))
	for value := range values {
		result[value] = struct{}{}
	}
	return result
}

// MustCompile is like Compile but panics when definition is invalid.
func MustCompile[S, E comparable, T any](definition Definition[S, E, T]) *Machine[S, E, T] {
	machine, err := Compile(definition)
	if err != nil {
		panic(err)
	}
	return machine
}

// ID reports the application-supplied stable definition identifier.
func (m *Machine[S, E, T]) ID() string {
	if m == nil {
		return ""
	}
	return m.id
}

// Initial reports the definition's canonical initial state. The boolean is
// false for a nil Machine.
func (m *Machine[S, E, T]) Initial() (S, bool) {
	if m == nil {
		var zero S
		return zero, false
	}
	return m.initial, true
}

// States iterates declared states in definition order.
func (m *Machine[S, E, T]) States() iter.Seq[StateInfo[S]] {
	return func(yield func(StateInfo[S]) bool) {
		if m == nil {
			return
		}
		for _, state := range m.states {
			if !yield(state) {
				return
			}
		}
	}
}

// Events iterates declared events in definition order.
func (m *Machine[S, E, T]) Events() iter.Seq[E] {
	return func(yield func(E) bool) {
		if m == nil {
			return
		}
		for _, event := range m.events {
			if !yield(event) {
				return
			}
		}
	}
}

// Transitions iterates declared transitions in definition order without
// exposing callbacks.
func (m *Machine[S, E, T]) Transitions() iter.Seq[TransitionInfo[S, E]] {
	return func(yield func(TransitionInfo[S, E]) bool) {
		if m == nil {
			return
		}
		for _, transition := range m.transitions {
			if !yield(transition) {
				return
			}
		}
	}
}

// Refused reports whether definition explicitly refuses event in state. A
// terminal state implicitly refuses every declared event.
func (m *Machine[S, E, T]) Refused(state S, event E) bool {
	if m == nil {
		return false
	}
	info, stateKnown := m.stateIndex[state]
	_, eventKnown := m.eventSet[event]
	if !stateKnown || !eventKnown {
		return false
	}
	if info.Terminal {
		return true
	}
	_, refused := m.refused[transitionKey[S, E]{state, event}]
	return refused
}
