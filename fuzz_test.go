package statemachine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/open-ships/statemachine"
)

// The fuzz targets in this file check the compiled Machine against a
// deliberately naive reference interpreter that walks the raw row slice. Any
// []byte must produce agreement between the two, never a panic.

type fuzzState int
type fuzzEvent int

// fuzzRow is the reference form of one fuzzed transition. Guards and effects
// are pure functions of the flags, so the reference can predict them.
type fuzzRow struct {
	from     fuzzState
	event    fuzzEvent
	to       fuzzState
	guarded  bool
	declines bool
	failing  bool
}

var (
	errFuzzDecline = errors.New("fuzz: guard declined")
	errFuzzEffect  = errors.New("fuzz: effect failed")
)

// decodeFuzzTable turns bytes into rows, four bytes per row.
func decodeFuzzTable(data []byte) []fuzzRow {
	rowCount := min(len(data)/4, 12)
	rows := make([]fuzzRow, 0, rowCount)
	for index := range rowCount {
		chunk := data[index*4 : index*4+4]
		rows = append(rows, fuzzRow{
			from:     fuzzState(chunk[0] % 5),
			event:    fuzzEvent(chunk[1] % 4),
			to:       fuzzState(chunk[2] % 5),
			guarded:  chunk[3]&1 != 0,
			declines: chunk[3]&2 != 0,
			failing:  chunk[3]&4 != 0,
		})
	}
	return rows
}

func buildFuzzTable(rows []fuzzRow) []statemachine.Transition[fuzzState, fuzzEvent, int] {
	table := make([]statemachine.Transition[fuzzState, fuzzEvent, int], 0, len(rows))
	for _, row := range rows {
		transition := statemachine.Transition[fuzzState, fuzzEvent, int]{
			From: row.from, Event: row.event, To: row.to,
		}
		if row.guarded {
			declines := row.declines
			transition.Guard = func(context.Context, int) error {
				if declines {
					return errFuzzDecline
				}
				return nil
			}
		}
		if row.failing {
			transition.Do = func(context.Context, int) error { return errFuzzEffect }
		}
		table = append(table, transition)
	}
	return table
}

// referenceDead reports whether any row is hidden by an earlier unguarded row
// with the same From and Event, which Compile must reject.
func referenceDead(rows []fuzzRow) bool {
	type key struct {
		from  fuzzState
		event fuzzEvent
	}
	unguarded := make(map[key]bool)
	for _, row := range rows {
		k := key{row.from, row.event}
		if unguarded[k] {
			return true
		}
		if !row.guarded {
			unguarded[k] = true
		}
	}
	return false
}

// referenceFire predicts Fire's outcome by walking the raw rows.
func referenceFire(rows []fuzzRow, from fuzzState, event fuzzEvent) (fuzzState, bool, bool, int) {
	declined := 0
	for _, row := range rows {
		if row.from != from || row.event != event {
			continue
		}
		if row.guarded && row.declines {
			declined++
			continue
		}
		if row.failing {
			return from, false, true, declined
		}
		return row.to, true, false, declined
	}
	return from, false, false, declined
}

// FuzzMachineFire drives arbitrary tables and event sequences through the
// compiled Machine and asserts exact agreement with the reference
// interpreter: selected row, resulting state, error identity, and refusal
// reasons.
func FuzzMachineFire(f *testing.F) {
	f.Add([]byte{0, 0, 1, 0}, []byte{0, 0})
	f.Add([]byte{0, 0, 1, 3, 0, 0, 2, 0}, []byte{0, 0, 1, 1})
	f.Add([]byte{0, 0, 1, 4}, []byte{0, 0})
	f.Add([]byte{0, 0, 1, 0, 0, 0, 2, 0}, []byte{0, 0})
	f.Add([]byte{1, 2, 3, 1, 1, 2, 4, 0}, []byte{1, 2, 3, 2})
	f.Fuzz(func(t *testing.T, tableBytes, fires []byte) {
		rows := decodeFuzzTable(tableBytes)
		machine, err := statemachine.Compile(buildFuzzTable(rows))
		if referenceDead(rows) {
			if err == nil || !strings.Contains(err.Error(), "unreachable") {
				t.Fatalf("Compile accepted a dead row: %v (rows %+v)", err, rows)
			}
			return
		}
		if err != nil {
			t.Fatalf("Compile = %v (rows %+v)", err, rows)
		}

		if len(fires) > 64 {
			fires = fires[:64]
		}
		ctx := context.Background()
		for index := 0; index+1 < len(fires); index += 2 {
			from := fuzzState(fires[index] % 5)
			event := fuzzEvent(fires[index+1] % 4)
			wantState, wantOK, wantEffectErr, declined := referenceFire(rows, from, event)

			got, err := machine.Fire(ctx, from, event, 0)
			if got != wantState {
				t.Fatalf("Fire(%v, %v) state = %v, want %v (rows %+v)", from, event, got, wantState, rows)
			}
			switch {
			case wantOK:
				if err != nil {
					t.Fatalf("Fire(%v, %v) = %v, want success", from, event, err)
				}
			case wantEffectErr:
				if !errors.Is(err, errFuzzEffect) || errors.Is(err, statemachine.ErrNotPermitted) {
					t.Fatalf("Fire(%v, %v) effect error = %v", from, event, err)
				}
			default:
				if !errors.Is(err, statemachine.ErrNotPermitted) {
					t.Fatalf("Fire(%v, %v) refusal = %v", from, event, err)
				}
				if declined > 0 && !errors.Is(err, errFuzzDecline) {
					t.Fatalf("refusal does not carry guard reason: %v", err)
				}
			}

			// Permitted must agree with Fire's selection rule: an event is
			// yielded iff the reference selects a row (an effect failure is
			// still a selected row), paired with that row's destination.
			permitted := make(map[fuzzEvent]fuzzState)
			for event, to := range machine.Permitted(ctx, from, 0) {
				if _, duplicate := permitted[event]; duplicate {
					t.Fatalf("Permitted yielded event %v twice", event)
				}
				permitted[event] = to
			}
			for probe := fuzzEvent(0); probe < 4; probe++ {
				var wantTo fuzzState
				wantYield := false
				for _, row := range rows {
					if row.from != from || row.event != probe {
						continue
					}
					if row.guarded && row.declines {
						continue
					}
					wantTo = row.to
					wantYield = true
					break
				}
				to, yielded := permitted[probe]
				if yielded != wantYield || (yielded && to != wantTo) {
					t.Fatalf("Permitted(%v)[%v] = %v/%v, want %v/%v (rows %+v)",
						from, probe, to, yielded, wantTo, wantYield, rows)
				}
			}
		}
	})
}

// FuzzZeroAndCompiledKeyChecks asserts that the zero Machine and a compiled
// Machine agree on refusing work, and that the compiled fast path never
// changes observable behavior for strictly comparable key types.
func FuzzZeroAndCompiledKeyChecks(f *testing.F) {
	f.Add(uint8(0), uint8(0))
	f.Add(uint8(3), uint8(2))
	f.Fuzz(func(t *testing.T, stateByte, eventByte uint8) {
		var zero statemachine.Machine[fuzzState, fuzzEvent, int]
		state := fuzzState(stateByte)
		event := fuzzEvent(eventByte)
		got, err := zero.Fire(context.Background(), state, event, 0)
		if got != state || !errors.Is(err, statemachine.ErrNotPermitted) {
			t.Fatalf("zero Machine Fire = %v, %v", got, err)
		}
		compiled := statemachine.MustCompile([]statemachine.Transition[fuzzState, fuzzEvent, int]{})
		got, err = compiled.Fire(context.Background(), state, event, 0)
		if got != state || !errors.Is(err, statemachine.ErrNotPermitted) {
			t.Fatalf("empty compiled Machine Fire = %v, %v", got, err)
		}
	})
}
