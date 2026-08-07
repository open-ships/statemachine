package supervised

import (
	"fmt"
	"io"
	"strings"
)

// WriteDOT writes a deterministic Graphviz DOT projection of the strict
// definition. Node identifiers follow state declaration order; labels retain
// state, event, and stable transition identifiers. Callback behavior is not
// serializable and therefore is not represented.
func (m *Machine[S, E, T]) WriteDOT(writer io.Writer) error {
	if m == nil {
		return ErrNilMachine
	}
	if writer == nil {
		return ErrNilWriter
	}

	nodes := make(map[S]string, len(m.states))
	var dot strings.Builder
	dot.WriteString("digraph supervised {\n")
	fmt.Fprintf(&dot, "    graph [label=\"%s\", labelloc=\"t\"];\n", dotEscape(m.id))
	dot.WriteString("    rankdir=LR;\n")
	dot.WriteString("    __start [shape=point];\n")
	for index, state := range m.states {
		node := fmt.Sprintf("s%d", index)
		nodes[state.Name] = node
		shape := "circle"
		if state.Terminal {
			shape = "doublecircle"
		}
		fmt.Fprintf(&dot, "    %s [label=\"%s\", shape=%s];\n", node, dotEscape(fmt.Sprint(state.Name)), shape)
	}
	fmt.Fprintf(&dot, "    __start -> %s;\n", nodes[m.initial])
	for _, transition := range m.transitions {
		label := fmt.Sprintf("%v [%s]", transition.Event, transition.ID)
		if transition.Guarded {
			label += " guarded"
		}
		if transition.Issues {
			label += " issue/verify"
		}
		fmt.Fprintf(
			&dot,
			"    %s -> %s [label=\"%s\"];\n",
			nodes[transition.From], nodes[transition.To], dotEscape(label),
		)
	}
	dot.WriteString("}\n")
	_, err := io.WriteString(writer, dot.String())
	return err
}

func dotEscape(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	value = strings.ReplaceAll(value, "\r", "\\r")
	return strings.ReplaceAll(value, "\n", "\\n")
}
