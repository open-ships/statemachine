package statechart

import (
	"fmt"
	"iter"
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
	state  S
	parent int
	enter  int
	exit   int
}

type shape[S comparable] struct {
	nodes []shapeNode[S]
	index map[S]int
}

func compileShape[S comparable](order []S, parents map[S]S) *shape[S] {
	result := &shape[S]{
		nodes: make([]shapeNode[S], len(order)),
		index: make(map[S]int, len(order)),
	}
	for index, state := range order {
		result.index[state] = index
		result.nodes[index] = shapeNode[S]{state: state, parent: -1}
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
	clock := 0
	var visit func(int)
	visit = func(index int) {
		result.nodes[index].enter = clock
		clock++
		for _, child := range children[index] {
			visit(child)
		}
		result.nodes[index].exit = clock
	}
	for index := range result.nodes {
		if result.nodes[index].parent == -1 {
			visit(index)
		}
	}
	return result
}

// Position is an immutable projection of one Chart at one exact committed
// active state. It remains valid after the Instance moves again.
type Position[S comparable] struct {
	shape  *shape[S]
	active int
}

// Position projects active onto c without evaluating Guards, resolving an
// Initial, or running actions. The boolean is false when c is nil or active is
// undeclared.
func (c *Chart[S, E, T]) Position(active S) (Position[S], bool) {
	if c == nil || c.shape == nil {
		return Position[S]{active: -1}, false
	}
	index, known := c.shape.index[active]
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
	return chart.Position(active)
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
// The boolean is false when state is undeclared or p is invalid.
func (p Position[S]) Status(state S) (Status, bool) {
	if p.shape == nil || p.active < 0 || p.active >= len(p.shape.nodes) {
		return StatusInactive, false
	}
	index, known := p.shape.index[state]
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
	index, known := c.shape.index[state]
	if !known {
		var zero S
		return zero, false
	}
	return c.destination(c.shape.nodes[index].state), true
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
		if _, known := c.shape.index[source]; !known {
			return
		}

		seen := make(map[E]struct{})
		for declaration := source; ; {
			for _, event := range c.events[declaration] {
				if _, duplicate := seen[event]; duplicate {
					continue
				}
				seen[event] = struct{}{}
				if !c.arrowsForEvent(source, event, yield) {
					return
				}
			}
			parent, ok := c.parent[declaration]
			if !ok {
				return
			}
			declaration = parent
		}
	}
}

func (c *Chart[S, E, T]) arrowsForEvent(source S, event E, yield func(Arrow[S, E]) bool) bool {
	for handler := source; ; {
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
		parent, ok := c.parent[handler]
		if !ok {
			return true
		}
		handler = parent
	}
}
