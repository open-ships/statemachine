package statechart

import (
	"fmt"
	"iter"

	"github.com/open-ships/statemachine/internal/keycheck"
)

// Status reports one state's membership in a Position.
type Status uint8

const (
	// StatusInactive identifies a declared state outside the active lineage.
	StatusInactive Status = iota
	// StatusEnclosing identifies an active ancestor of the exact active state.
	StatusEnclosing
	// StatusActive identifies the exact active state.
	StatusActive
)

func (s Status) String() string {
	switch s {
	case StatusInactive:
		return "inactive"
	case StatusEnclosing:
		return "enclosing"
	case StatusActive:
		return "active"
	default:
		return fmt.Sprintf("Status(%d)", uint8(s))
	}
}

type shapeNode[S comparable] struct {
	state       S
	parent      int
	depth       int
	enter       int
	exit        int
	destination int
}

type shape[S comparable] struct {
	nodes     []shapeNode[S]
	index     map[S]int
	reflexive bool
}

func compileShape[S comparable](order []S, parents, initials map[S]S) *shape[S] {
	result := &shape[S]{
		nodes:     make([]shapeNode[S], len(order)),
		index:     make(map[S]int, len(order)),
		reflexive: keycheck.ReflexiveType[S](),
	}
	for index, state := range order {
		result.index[state] = index
		result.nodes[index] = shapeNode[S]{state: state, parent: -1, destination: index}
	}

	children := make([][]int, len(order))
	for childIndex, child := range order {
		parent, hasParent := parents[child]
		if !hasParent {
			continue
		}
		parentIndex := result.index[parent]
		result.nodes[childIndex].parent = parentIndex
		children[parentIndex] = append(children[parentIndex], childIndex)
	}
	// Preorder assigns membership intervals and depths; reversing it resolves
	// initial chains and subtree ends after every descendant. Both passes are
	// linear and iterative, including for a deeply nested generated chart.
	preorder := make([]int, 0, len(order))
	stack := make([]int, 0)
	for index := range result.nodes {
		if result.nodes[index].parent == -1 {
			stack = append(stack, index)
		}
	}
	for len(stack) != 0 {
		index := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		node := &result.nodes[index]
		node.enter = len(preorder)
		preorder = append(preorder, index)
		if node.parent != -1 {
			node.depth = result.nodes[node.parent].depth + 1
		}
		stack = append(stack, children[index]...)
	}
	for cursor := len(preorder) - 1; cursor >= 0; cursor-- {
		index := preorder[cursor]
		node := &result.nodes[index]
		node.exit = node.enter + 1
		for _, child := range children[index] {
			node.exit = max(node.exit, result.nodes[child].exit)
		}
		if initial, ok := initials[node.state]; ok {
			node.destination = result.nodes[result.index[initial]].destination
		}
	}
	return result
}

func (s *shape[S]) stateIndex(state S) (int, bool) {
	if s == nil || !s.reflexive && !keycheck.Value(state) {
		return 0, false
	}
	index, known := s.index[state]
	return index, known
}

func (s *shape[S]) leastCommonAncestor(left, right int) int {
	for s.nodes[left].depth > s.nodes[right].depth {
		left = s.nodes[left].parent
	}
	for s.nodes[right].depth > s.nodes[left].depth {
		right = s.nodes[right].parent
	}
	for left != right {
		left = s.nodes[left].parent
		right = s.nodes[right].parent
	}
	return left
}

// paths uses only the compiled hierarchy. Its single allocation holds both
// lifecycle paths, with no per-transition ancestor maps or cached row products.
func (s *shape[S]) paths(source, target, destination S, reentry bool) (exits, entries []S) {
	from := s.index[source]
	to := s.index[destination]
	stop := s.index[target]
	if reentry {
		stop = s.nodes[stop].parent
	} else {
		stop = s.leastCommonAncestor(from, stop)
	}
	depth := -1
	if stop != -1 {
		depth = s.nodes[stop].depth
	}
	exitCount := s.nodes[from].depth - depth
	entryCount := s.nodes[to].depth - depth
	if exitCount+entryCount == 0 {
		return nil, nil
	}
	path := make([]S, exitCount+entryCount)
	exits, entries = path[:exitCount:exitCount], path[exitCount:]
	for index := range exits {
		exits[index] = s.nodes[from].state
		from = s.nodes[from].parent
	}
	for index := len(entries) - 1; index >= 0; index-- {
		entries[index] = s.nodes[to].state
		to = s.nodes[to].parent
	}
	return exits, entries
}

// Position is an immutable projection of one Chart at one exact committed
// active state. It remains valid after the Instance moves again.
type Position[S comparable] struct {
	shape  *shape[S]
	active int
}

// Position projects active onto c without evaluating Guards, resolving an
// Initial, or running actions. The boolean is false when c is nil or active is
// undeclared or an invalid key.
func (c *Chart[S, E, T]) Position(active S) (Position[S], bool) {
	if c == nil || c.shape == nil {
		return Position[S]{active: -1}, false
	}
	index, known := c.shape.stateIndex(active)
	if !known {
		return Position[S]{active: -1}, false
	}
	return Position[S]{shape: c.shape, active: index}, true
}

// Position returns an atomic snapshot of the Instance's committed position.
// The boolean is false for the zero Instance.
func (i *Instance[S, E, T]) Position() (Position[S], bool) {
	i.mu.RLock()
	chart := i.chart
	active := i.state
	i.mu.RUnlock()
	return (&chart).Position(active)
}

// Active reports the exact active state. The boolean is false for a zero or
// otherwise invalid Position.
func (p Position[S]) Active() (S, bool) {
	if p.shape == nil || p.active < 0 || p.active >= len(p.shape.nodes) {
		var zero S
		return zero, false
	}
	return p.shape.nodes[p.active].state, true
}

// Path iterates the active lineage from the exact active state toward its root.
func (p Position[S]) Path() iter.Seq[S] {
	return func(yield func(S) bool) {
		if p.shape == nil || p.active < 0 || p.active >= len(p.shape.nodes) {
			return
		}
		for index := p.active; index != -1; index = p.shape.nodes[index].parent {
			if !yield(p.shape.nodes[index].state) {
				return
			}
		}
	}
}

// Status reports whether state is inactive, encloses Active, or is Active.
// The boolean is false when state is undeclared, is an invalid key, or p is invalid.
func (p Position[S]) Status(state S) (Status, bool) {
	if p.shape == nil || p.active < 0 || p.active >= len(p.shape.nodes) {
		return StatusInactive, false
	}
	index, known := p.shape.stateIndex(state)
	if !known {
		return StatusInactive, false
	}
	if index == p.active {
		return StatusActive, true
	}
	node := p.shape.nodes[index]
	active := p.shape.nodes[p.active]
	if node.enter <= active.enter && active.exit <= node.exit {
		return StatusEnclosing, true
	}
	return StatusInactive, true
}

// States iterates the compiled Chart's states in declaration order.
func (c *Chart[S, E, T]) States() iter.Seq[S] {
	return func(yield func(S) bool) {
		if c == nil || c.shape == nil {
			return
		}
		for _, item := range c.shape.nodes {
			if !yield(item.state) {
				return
			}
		}
	}
}

// Destination reports the exact state reached by following state's Initial
// chain. The boolean is false when c is nil or state is undeclared.
func (c *Chart[S, E, T]) Destination(state S) (S, bool) {
	if c == nil || c.shape == nil {
		var zero S
		return zero, false
	}
	index, known := c.shape.stateIndex(state)
	if !known {
		var zero S
		return zero, false
	}
	return c.shape.nodes[c.shape.nodes[index].destination].state, true
}

// Arrow is one statically possible transition selected from Source. Guarded
// alternatives for the same Event are returned separately in selection order.
type Arrow[S, E comparable] struct {
	Source      S
	Handler     S
	Target      S
	Destination S
	Event       E
	Kind        Kind
	Guarded     bool
}

// Arrows iterates every transition that could be selected from source without
// evaluating Guards. It follows inherited handlers and stops considering
// higher handlers for an Event after an unconditional row. Unlike Permitted,
// Arrows may yield the same Event more than once.
func (c *Chart[S, E, T]) Arrows(source S) iter.Seq[Arrow[S, E]] {
	return func(yield func(Arrow[S, E]) bool) {
		if c == nil || c.shape == nil {
			return
		}
		start, known := c.shape.stateIndex(source)
		if !known {
			return
		}

		seen := make(map[E]struct{})
		for index := start; index != -1; index = c.shape.nodes[index].parent {
			declaration := c.shape.nodes[index].state
			for _, event := range c.events[declaration] {
				if _, duplicate := seen[event]; duplicate {
					continue
				}
				seen[event] = struct{}{}
				if !c.arrowsForEvent(source, event, yield) {
					return
				}
			}
		}
	}
}

func (c *Chart[S, E, T]) arrowsForEvent(source S, event E, yield func(Arrow[S, E]) bool) bool {
	for index := c.shape.index[source]; index != -1; index = c.shape.nodes[index].parent {
		handler := c.shape.nodes[index].state
		for _, transition := range c.rows[transitionKey[S, E]{handler, event}] {
			destination := source
			if transition.Kind != Internal {
				destination = c.destination(transition.To)
			}
			if !yield(Arrow[S, E]{
				Source: source, Handler: handler, Target: transition.To,
				Destination: destination, Event: event, Kind: transition.Kind,
				Guarded: transition.Guard != nil,
			}) {
				return false
			}
			if transition.Guard == nil {
				return true
			}
		}
	}
	return true
}
