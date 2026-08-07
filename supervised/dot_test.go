package supervised

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestWriteDOT(t *testing.T) {
	definition := validDefinition()
	definition.ID = "test-\"machine\"/v1"
	definition.Transitions[0].Guard = func(_ context.Context, _ Change[testState, testEvent], _ *testData) error { return nil }
	machine := MustCompile(definition)
	var output bytes.Buffer
	if err := machine.WriteDOT(&output); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`graph [label="test-\"machine\"/v1"`,
		`__start -> s0`,
		`s0 [label="idle", shape=circle]`,
		`s1 [label="running", shape=doublecircle]`,
		`s0 -> s1 [label="start [start-running] guarded issue/verify"]`,
	} {
		if !strings.Contains(output.String(), want) {
			t.Errorf("DOT does not contain %q:\n%s", want, output.String())
		}
	}
}

func TestWriteDOTErrorsAndEscaping(t *testing.T) {
	var machine *Machine[testState, testEvent, *testData]
	if err := machine.WriteDOT(&bytes.Buffer{}); !errors.Is(err, ErrNilMachine) {
		t.Fatalf("nil Machine = %v", err)
	}
	machine = MustCompile(validDefinition())
	if err := machine.WriteDOT(nil); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("nil writer = %v", err)
	}
	if got := dotEscape("a\\b\r\n\"c"); got != `a\\b\r\n\"c` {
		t.Fatalf("dotEscape = %q", got)
	}
}
