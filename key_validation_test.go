package statemachine_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/open-ships/statemachine"
)

func checkInvalidTableKeys[K comparable](t *testing.T, valid, invalid K) {
	t.Helper()
	rows := []statemachine.Transition[K, K, struct{}]{
		{From: invalid, Event: valid, To: valid},
		{From: valid, Event: invalid, To: valid},
		{From: valid, Event: valid, To: invalid},
	}
	for index, row := range rows {
		if _, err := statemachine.Compile([]statemachine.Transition[K, K, struct{}]{row}); !errors.Is(err, statemachine.ErrInvalidKey) {
			t.Errorf("row %d: Compile = %v, want ErrInvalidKey", index, err)
		}
	}
}

func TestCompileRejectsNaNBearingKeys(t *testing.T) {
	nan := math.NaN()
	checkInvalidTableKeys(t, float64(0), nan)
	checkInvalidTableKeys(t, float32(0), float32(nan))
	checkInvalidTableKeys(t, complex128(0), complex(nan, 0))
	checkInvalidTableKeys(t, complex64(0), complex(float32(0), float32(nan)))
	checkInvalidTableKeys(t, [1]float64{}, [1]float64{nan})
	type nested struct{ Value [1]complex128 }
	checkInvalidTableKeys(t, nested{}, nested{[1]complex128{complex(0, nan)}})
}

func TestFloatingKeyQueriesRejectNaNBeforeCallbacks(t *testing.T) {
	calls := 0
	machine := statemachine.MustCompile([]statemachine.Transition[float64, float64, struct{}]{{
		From: 0, Event: 0, To: 1,
		Guard: func(context.Context, struct{}) error { calls++; return nil },
		Do:    func(context.Context, struct{}) error { calls++; return nil },
	}})
	ctx := context.Background()
	if _, err := machine.Next(ctx, math.NaN(), 0, struct{}{}); !errors.Is(err, statemachine.ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, err := machine.Next(ctx, 0, math.NaN(), struct{}{}); !errors.Is(err, statemachine.ErrInvalidKey) {
		t.Fatal(err)
	}
	for range machine.Permitted(ctx, math.NaN(), struct{}{}) {
		t.Fatal("invalid state yielded an event")
	}
	instance := statemachine.NewInstance(machine, 0)
	if state, err := instance.Fire(ctx, math.NaN(), struct{}{}); state != 0 || !errors.Is(err, statemachine.ErrInvalidKey) {
		t.Fatalf("Fire = %v, %v", state, err)
	}
	if calls != 0 {
		t.Fatalf("invalid keys ran %d callbacks", calls)
	}
	if state, err := instance.Fire(ctx, 0, struct{}{}); state != 1 || err != nil || calls != 2 {
		t.Fatalf("valid Fire = %v, %v; calls %d", state, err, calls)
	}
	var zero statemachine.Machine[any, any, struct{}]
	for _, key := range []any{math.NaN(), [1]float64{math.NaN()}, []int{1}} {
		if _, err := zero.Next(ctx, nil, key, struct{}{}); !errors.Is(err, statemachine.ErrInvalidKey) {
			t.Fatalf("zero Next(%v) = %v", key, err)
		}
	}
}
