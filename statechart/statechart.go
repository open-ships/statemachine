// Package statechart runs hierarchical statecharts.
//
// A Chart is an immutable definition shared by any number of Instances. Each
// Instance owns one active state. Transitions declared on a parent state are
// inherited by its descendants. Selection starts at the active state and walks
// toward its ancestors; guarded rows that decline do not hide an applicable
// row higher in the hierarchy.
//
// External transitions run exit actions from the active state toward, but not
// including, the least common ancestor of Source and Target. The transition
// effect then runs, the final initial-resolved Destination is committed, and
// entry actions run from that ancestor toward the destination. Internal
// transitions run only their effect. Reentry transitions exit from the active
// state through the state that handled the event, then re-enter that state and
// descend through its initial states. A composite state without an Initial may
// itself be active.
//
// Exit and transition-effect failures leave the old state committed. Entry
// actions begin only after the destination is committed, so an entry failure
// leaves the new state committed. [ActionError] exposes both the phase and that
// commit status. Actions completed before an error or panic are not rolled
// back. An Instance rejects overlapping and same-instance recursive Fire calls
// with [ErrInFlight].
package statechart

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"

	"github.com/open-ships/statemachine"
)

// ErrNotPermitted is [statemachine.ErrNotPermitted].
var ErrNotPermitted = statemachine.ErrNotPermitted

// ErrInFlight is [statemachine.ErrInFlight].
var ErrInFlight = statemachine.ErrInFlight

// Kind determines which states a transition exits and enters.
type Kind uint8

const (
	// External exits states up to the least common ancestor of the active
	// state and Target, then enters states down to Destination. The common
	// ancestor itself is not exited or entered, even when it is Handler; use
	// Reentry when that state's lifecycle must run again.
	External Kind = iota

	// Internal runs only the transition effect. It neither exits nor enters a
	// state, and the active state does not change.
	Internal

	// Reentry exits through the state that handled the event, then re-enters
	// it and follows its initial-state chain.
	Reentry
)

func (k Kind) String() string {
	switch k {
	case External:
		return "external"
	case Internal:
		return "internal"
	case Reentry:
		return "reentry"
	default:
		return fmt.Sprintf("Kind(%d)", uint8(k))
	}
}

// Info describes the transition being performed. Source is the active state
// before firing. Handler is the state whose row handled Event, which may be an
// ancestor of Source. Target is that row's To value. Destination is the state
// active after following Target's initial-state chain.
type Info[S, E comparable] struct {
	Source      S
	Handler     S
	Target      S
	Destination S
	Event       E
	Kind        Kind
}

// Action is a transition, entry, or exit effect.
type Action[S, E comparable, T any] func(context.Context, Info[S, E], T) error

// Guard reports whether a transition applies: nil to apply, any error to
// decline. Info identifies both the active Source and the possibly inherited
// Handler, so one parent declaration can make decisions about its descendants
// without duplicating the active state in T.
//
// A Guard must be pure. It may run for a row that loses, and [Instance.Permitted]
// runs it while computing an affordance snapshot.
type Guard[S, E comparable, T any] func(context.Context, Info[S, E], T) error

// State declares a state and its lifecycle actions. Actions run in slice
// order. A nil action is ignored.
type State[S, E comparable, T any] struct {
	Name  S
	Entry []Action[S, E, T]
	Exit  []Action[S, E, T]
}

// Substate makes Child an immediate child of Parent.
type Substate[S comparable] struct {
	Child  S
	Parent S
}

// Initial selects the immediate Child entered whenever Parent is entered.
type Initial[S comparable] struct {
	Parent S
	Child  S
}

// Transition declares one event handler. Guard returns nil when the row
// applies. Rows with the same From and Event are considered in declaration
// order.
type Transition[S, E comparable, T any] struct {
	From  S
	Event E
	To    S
	Kind  Kind
	Guard Guard[S, E, T]
	Do    Action[S, E, T]
}

// Definition is the input to [Compile]. Every state referenced by a
// transition, hierarchy edge, or initial edge must occur exactly once in
// States.
type Definition[S, E comparable, T any] struct {
	States      []State[S, E, T]
	Substates   []Substate[S]
	Initials    []Initial[S]
	Transitions []Transition[S, E, T]
}

type transitionKey[S, E comparable] struct {
	state S
	event E
}

type stateActions[S, E comparable, T any] struct {
	entry []Action[S, E, T]
	exit  []Action[S, E, T]
}

// Chart is an immutable compiled statechart definition. It is safe for
// concurrent use. A Chart does not itself own an active state; use [Chart.New]
// to create an Instance.
type Chart[S, E comparable, T any] struct {
	states  map[S]stateActions[S, E, T]
	order   []S
	parent  map[S]S
	initial map[S]S
	rows    map[transitionKey[S, E]][]Transition[S, E, T]
	events  map[S][]E
}

// Compile validates and copies definition. It rejects duplicate or undeclared
// states, multiple parents, hierarchy cycles, multiple or non-child Initials,
// invalid transition kinds or targets, and rows hidden by an earlier unguarded
// row with the same From and Event. It reports all independently discoverable
// defects together with [errors.Join].
func Compile[S, E comparable, T any](definition Definition[S, E, T]) (*Chart[S, E, T], error) {
	c := &Chart[S, E, T]{
		states:  make(map[S]stateActions[S, E, T], len(definition.States)),
		order:   make([]S, 0, len(definition.States)),
		parent:  make(map[S]S, len(definition.Substates)),
		initial: make(map[S]S, len(definition.Initials)),
		rows:    make(map[transitionKey[S, E]][]Transition[S, E, T], len(definition.Transitions)),
		events:  make(map[S][]E),
	}
	var problems []error
	stateAt := make(map[S]int, len(definition.States))
	for index, state := range definition.States {
		if previous, exists := stateAt[state.Name]; exists {
			problems = append(problems, fmt.Errorf(
				"statechart: states[%d] duplicates state %v declared at states[%d]",
				index, state.Name, previous))
			continue
		}
		stateAt[state.Name] = index
		c.order = append(c.order, state.Name)
		c.states[state.Name] = stateActions[S, E, T]{
			entry: append([]Action[S, E, T](nil), state.Entry...),
			exit:  append([]Action[S, E, T](nil), state.Exit...),
		}
	}

	parentAt := make(map[S]int, len(definition.Substates))
	for index, edge := range definition.Substates {
		childKnown := knownState(stateAt, edge.Child, "substates", index, "Child", &problems)
		parentKnown := knownState(stateAt, edge.Parent, "substates", index, "Parent", &problems)
		if edge.Child == edge.Parent && childKnown && parentKnown {
			problems = append(problems, fmt.Errorf(
				"statechart: substates[%d] makes state %v its own parent", index, edge.Child))
			continue
		}
		if previous, exists := parentAt[edge.Child]; exists {
			problems = append(problems, fmt.Errorf(
				"statechart: substates[%d] gives state %v a second parent; first declared at substates[%d]",
				index, edge.Child, previous))
			continue
		}
		parentAt[edge.Child] = index
		if childKnown && parentKnown {
			c.parent[edge.Child] = edge.Parent
		}
	}
	problems = append(problems, hierarchyCycles(c.order, c.parent)...)

	initialAt := make(map[S]int, len(definition.Initials))
	for index, edge := range definition.Initials {
		parentKnown := knownState(stateAt, edge.Parent, "initials", index, "Parent", &problems)
		childKnown := knownState(stateAt, edge.Child, "initials", index, "Child", &problems)
		if previous, exists := initialAt[edge.Parent]; exists {
			problems = append(problems, fmt.Errorf(
				"statechart: initials[%d] gives state %v a second initial child; first declared at initials[%d]",
				index, edge.Parent, previous))
			continue
		}
		initialAt[edge.Parent] = index
		if !parentKnown || !childKnown {
			continue
		}
		parent, direct := c.parent[edge.Child]
		if !direct || parent != edge.Parent {
			problems = append(problems, fmt.Errorf(
				"statechart: initials[%d] child %v is not an immediate child of parent %v",
				index, edge.Child, edge.Parent))
			continue
		}
		c.initial[edge.Parent] = edge.Child
	}

	unguarded := make(map[transitionKey[S, E]]int, len(definition.Transitions))
	for index, transition := range definition.Transitions {
		knownState(stateAt, transition.From, "transitions", index, "From", &problems)
		knownState(stateAt, transition.To, "transitions", index, "To", &problems)
		switch transition.Kind {
		case External:
			if transition.From == transition.To {
				problems = append(problems, fmt.Errorf(
					"statechart: transitions[%d] is external but From and To are both %v; use Internal or Reentry",
					index, transition.From))
			}
		case Internal, Reentry:
			if transition.From != transition.To {
				problems = append(problems, fmt.Errorf(
					"statechart: transitions[%d] is %s but From %v differs from To %v",
					index, transition.Kind, transition.From, transition.To))
			}
		default:
			problems = append(problems, fmt.Errorf(
				"statechart: transitions[%d] has invalid kind %v", index, transition.Kind))
		}

		key := transitionKey[S, E]{transition.From, transition.Event}
		if previous, dead := unguarded[key]; dead {
			problems = append(problems, fmt.Errorf(
				"statechart: transitions[%d] (%v on %v) is unreachable: transitions[%d] has the same From and Event and no Guard",
				index, transition.From, transition.Event, previous))
		}
		if transition.Guard == nil {
			if _, already := unguarded[key]; !already {
				unguarded[key] = index
			}
		}
		if _, seen := c.rows[key]; !seen {
			c.events[transition.From] = append(c.events[transition.From], transition.Event)
		}
		c.rows[key] = append(c.rows[key], transition)
	}

	if len(problems) != 0 {
		return nil, errors.Join(problems...)
	}
	return c, nil
}

func knownState[S comparable](states map[S]int, value S, section string, index int, field string, problems *[]error) bool {
	if _, known := states[value]; known {
		return true
	}
	*problems = append(*problems, fmt.Errorf(
		"statechart: %s[%d].%s names undeclared state %v", section, index, field, value))
	return false
}

func hierarchyCycles[S comparable](order []S, parent map[S]S) []error {
	const (
		unseen uint8 = iota
		visiting
		done
	)
	color := make(map[S]uint8, len(order))
	stack := make([]S, 0, len(order))
	positions := make(map[S]int, len(order))
	var problems []error
	var visit func(S)
	visit = func(state S) {
		switch color[state] {
		case done:
			return
		case visiting:
			start := positions[state]
			cycle := append(append([]S(nil), stack[start:]...), state)
			parts := make([]string, len(cycle))
			for i, item := range cycle {
				parts[i] = fmt.Sprint(item)
			}
			problems = append(problems, fmt.Errorf(
				"statechart: hierarchy contains a cycle: %s", strings.Join(parts, " -> ")))
			return
		}
		color[state] = visiting
		positions[state] = len(stack)
		stack = append(stack, state)
		if next, ok := parent[state]; ok {
			visit(next)
		}
		stack = stack[:len(stack)-1]
		delete(positions, state)
		color[state] = done
	}
	for _, state := range order {
		if color[state] == unseen {
			visit(state)
		}
	}
	return problems
}

// MustCompile is like [Compile] but panics when definition is invalid. It is
// intended for definitions written as program text.
func MustCompile[S, E comparable, T any](definition Definition[S, E, T]) *Chart[S, E, T] {
	chart, err := Compile(definition)
	if err != nil {
		panic(err)
	}
	return chart
}

// New creates an Instance restored at initial. If initial has an initial-state
// chain, New resolves it without running entry actions. A composite state with
// no Initial remains active itself.
func (c *Chart[S, E, T]) New(initial S) (*Instance[S, E, T], error) {
	if c == nil {
		return nil, errors.New("statechart: cannot create an instance from a nil chart")
	}
	if _, known := c.states[initial]; !known {
		return nil, fmt.Errorf("statechart: initial state %v is undeclared", initial)
	}
	return &Instance[S, E, T]{chart: c, state: c.destination(initial)}, nil
}

func (c *Chart[S, E, T]) destination(state S) S {
	for {
		next, ok := c.initial[state]
		if !ok {
			return state
		}
		state = next
	}
}

type selected[S, E comparable, T any] struct {
	transition Transition[S, E, T]
	info       Info[S, E]
}

func (c *Chart[S, E, T]) selectTransition(ctx context.Context, source S, event E, data T) (selected[S, E, T], []error, bool) {
	if c == nil {
		return selected[S, E, T]{}, nil, false
	}
	var reasons []error
	for state := source; ; {
		for _, transition := range c.rows[transitionKey[S, E]{state, event}] {
			destination := source
			if transition.Kind != Internal {
				destination = c.destination(transition.To)
			}
			info := Info[S, E]{
				Source: source, Handler: state, Target: transition.To,
				Destination: destination, Event: event, Kind: transition.Kind,
			}
			if transition.Guard != nil {
				if err := transition.Guard(ctx, info, data); err != nil {
					reasons = append(reasons, err)
					continue
				}
			}
			return selected[S, E, T]{transition: transition, info: info}, reasons, true
		}
		parent, ok := c.parent[state]
		if !ok {
			return selected[S, E, T]{}, reasons, false
		}
		state = parent
	}
}

func (c *Chart[S, E, T]) ancestors(state S) []S {
	path := []S{state}
	for {
		parent, ok := c.parent[state]
		if !ok {
			return path
		}
		path = append(path, parent)
		state = parent
	}
}

func (c *Chart[S, E, T]) leastCommonAncestor(source, target S) (S, bool) {
	targets := make(map[S]struct{})
	for _, state := range c.ancestors(target) {
		targets[state] = struct{}{}
	}
	for _, state := range c.ancestors(source) {
		if _, common := targets[state]; common {
			return state, true
		}
	}
	var zero S
	return zero, false
}

func (c *Chart[S, E, T]) externalPaths(source, target, destination S) (exits, entries []S) {
	lca, common := c.leastCommonAncestor(source, target)
	for state := source; ; {
		if common && state == lca {
			break
		}
		exits = append(exits, state)
		parent, ok := c.parent[state]
		if !ok {
			break
		}
		state = parent
	}

	up := c.ancestors(destination)
	for _, state := range up {
		if common && state == lca {
			break
		}
		entries = append(entries, state)
	}
	reverse(entries)
	return exits, entries
}

func (c *Chart[S, E, T]) reentryPaths(source, handler, destination S) (exits, entries []S) {
	for state := source; ; {
		exits = append(exits, state)
		if state == handler {
			break
		}
		state = c.parent[state]
	}
	for state := destination; ; {
		entries = append(entries, state)
		if state == handler {
			break
		}
		state = c.parent[state]
	}
	reverse(entries)
	return exits, entries
}

func reverse[S any](values []S) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

// Phase identifies the lifecycle phase in which an Action failed.
type Phase uint8

const (
	// PhaseExit identifies an exit action.
	PhaseExit Phase = iota
	// PhaseEffect identifies a transition's Do action.
	PhaseEffect
	// PhaseEntry identifies an entry action.
	PhaseEntry
)

func (p Phase) String() string {
	switch p {
	case PhaseExit:
		return "exit"
	case PhaseEffect:
		return "effect"
	case PhaseEntry:
		return "entry"
	default:
		return fmt.Sprintf("Phase(%d)", uint8(p))
	}
}

// ActionError reports a lifecycle action failure. Committed says whether the
// Instance had published Destination before the action failed. Exit and effect
// errors are uncommitted; entry errors are committed.
type ActionError struct {
	Phase     Phase
	Committed bool
	Err       error
}

func (e *ActionError) Error() string {
	if e == nil {
		return "<nil>"
	}
	status := "before state commit"
	if e.Committed {
		status = "after state commit"
	}
	return fmt.Sprintf("statechart: %s action failed %s: %v", e.Phase, status, e.Err)
}

// Unwrap returns the action's error.
func (e *ActionError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Instance owns the active state of one aggregate. The zero Instance is usable:
// it starts in the zero state with an empty Chart and refuses every event. An
// Instance must not be copied after first use.
type Instance[S, E comparable, T any] struct {
	chart    *Chart[S, E, T]
	mu       sync.RWMutex
	state    S
	inFlight bool
}

// State returns the committed active state. During exits and the transition
// effect it returns Source; during entries it returns Destination.
func (i *Instance[S, E, T]) State() S {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.state
}

// Fire applies event to the active state.
//
// It selects rows in declaration order at the active state, then repeats at
// each ancestor until a Guard applies. If none applies, Fire returns a refusal
// that wraps [ErrNotPermitted] and every Guard reason, and leaves the state
// unchanged.
//
// For External and Reentry transitions, exit actions run child-to-parent, then
// Do runs, then Destination is committed, then entry actions run
// parent-to-child. Actions on one state run in their declaration order. An exit
// or Do error returns an [ActionError] with Committed false and leaves Source
// active. An entry error returns one with Committed true and leaves Destination
// active. Internal runs only Do and never changes the active state.
//
// A panic propagates after the in-flight marker is cleared. State remains at
// whichever side of the commit point the panic occurred; effects and lifecycle
// actions already performed are not rolled back. Concurrent and same-Instance
// reentrant calls fail immediately with [ErrInFlight].
//
// Fire does not interpret ctx cancellation itself; it passes ctx to Guards and
// Actions, which decide whether cancellation should decline or fail the event.
func (i *Instance[S, E, T]) Fire(ctx context.Context, event E, data T) error {
	i.mu.Lock()
	if i.inFlight {
		i.mu.Unlock()
		return ErrInFlight
	}
	i.inFlight = true
	source := i.state
	i.mu.Unlock()

	defer func() {
		i.mu.Lock()
		i.inFlight = false
		i.mu.Unlock()
	}()

	selection, reasons, ok := i.chart.selectTransition(ctx, source, event, data)
	if !ok {
		return &refusal[S, E]{source, event, append([]error{ErrNotPermitted}, reasons...)}
	}
	transition := selection.transition
	info := selection.info
	destination := info.Destination

	if transition.Kind == Internal {
		if err := runAction(ctx, transition.Do, info, data); err != nil {
			return &ActionError{Phase: PhaseEffect, Err: err}
		}
		return nil
	}

	var exits, entries []S
	if transition.Kind == Reentry {
		exits, entries = i.chart.reentryPaths(source, info.Handler, destination)
	} else {
		exits, entries = i.chart.externalPaths(source, transition.To, destination)
	}
	for _, state := range exits {
		for _, action := range i.chart.states[state].exit {
			if err := runAction(ctx, action, info, data); err != nil {
				return &ActionError{Phase: PhaseExit, Err: err}
			}
		}
	}
	if err := runAction(ctx, transition.Do, info, data); err != nil {
		return &ActionError{Phase: PhaseEffect, Err: err}
	}

	i.mu.Lock()
	i.state = destination
	i.mu.Unlock()
	for _, state := range entries {
		for _, action := range i.chart.states[state].entry {
			if err := runAction(ctx, action, info, data); err != nil {
				return &ActionError{Phase: PhaseEntry, Committed: true, Err: err}
			}
		}
	}
	return nil
}

func runAction[S, E comparable, T any](ctx context.Context, action Action[S, E, T], info Info[S, E], data T) error {
	if action == nil {
		return nil
	}
	return action(ctx, info, data)
}

type permit[S, E comparable] struct {
	event E
	to    S
}

// Permitted returns an iterator over the events currently accepted, each with
// its final destination after initial-state descent. It snapshots the committed
// state first, then evaluates every relevant Guard before returning. No
// Instance lock is held while Guards run.
//
// Each event appears at most once. Ordering starts with events first declared
// on the active state, then events first declared on each ancestor. Selection
// still considers every same-event row and guarded fallback in the same order
// as Fire.
//
// Consequently, a concurrent Fire does not block Permitted: the snapshot sees
// Source during exit and Do, or Destination during entry. The caller must
// synchronize mutable data in T against actions, and Guards must be pure.
func (i *Instance[S, E, T]) Permitted(ctx context.Context, data T) iter.Seq2[E, S] {
	i.mu.RLock()
	source := i.state
	chart := i.chart
	i.mu.RUnlock()
	if chart == nil {
		return func(func(E, S) bool) {}
	}

	seen := make(map[E]struct{})
	var permits []permit[S, E]
	for state := source; ; {
		for _, event := range chart.events[state] {
			if _, duplicate := seen[event]; duplicate {
				continue
			}
			seen[event] = struct{}{}
			selection, _, ok := chart.selectTransition(ctx, source, event, data)
			if !ok {
				continue
			}
			permits = append(permits, permit[S, E]{event, selection.info.Destination})
		}
		parent, ok := chart.parent[state]
		if !ok {
			break
		}
		state = parent
	}
	return func(yield func(E, S) bool) {
		for _, permit := range permits {
			if !yield(permit.event, permit.to) {
				return
			}
		}
	}
}

type refusal[S, E comparable] struct {
	state  S
	event  E
	errors []error
}

func (r *refusal[S, E]) Error() string {
	var text strings.Builder
	fmt.Fprintf(&text, "statechart: %v in state %v: %s", r.event, r.state, ErrNotPermitted)
	for index, reason := range r.errors[1:] {
		if index == 0 {
			text.WriteString(": ")
		} else {
			text.WriteString("; ")
		}
		text.WriteString(reason.Error())
	}
	return text.String()
}

func (r *refusal[S, E]) Unwrap() []error { return r.errors }
