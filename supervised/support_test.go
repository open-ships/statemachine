package supervised

// The shared test Machine declares external Issue actions, so plain New and
// Restore refuse it by design. These helpers opt into the explicitly named
// volatile mode; construction-policy behavior itself is tested separately.

func newUnjournaled(
	machine *Machine[testState, testEvent, *testData], limits Limits,
) (*Supervisor[testState, testEvent, *testData], error) {
	return NewWithOptions(machine, Options[testState, testEvent]{Limits: limits, Unjournaled: true})
}

func newUnjournaledWithClock(
	machine *Machine[testState, testEvent, *testData], limits Limits, clock Clock,
) (*Supervisor[testState, testEvent, *testData], error) {
	return NewWithOptions(machine, Options[testState, testEvent]{
		Limits: limits, Clock: clock, Unjournaled: true,
	})
}

func restoreUnjournaled(
	machine *Machine[testState, testEvent, *testData], snapshot Snapshot[testState, testEvent], limits Limits,
) (*Supervisor[testState, testEvent, *testData], error) {
	return RestoreWithOptions(machine, snapshot, Options[testState, testEvent]{
		Limits: limits, Unjournaled: true,
	})
}
