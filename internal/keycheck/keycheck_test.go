package keycheck

import (
	"math"
	"testing"
)

func TestStrictTypeRejectsInterfacesAtEveryKeyDepth(t *testing.T) {
	type nested struct{ Value any }
	if !StrictType[string]() || !StrictType[*nested]() || !StrictType[float64]() {
		t.Fatal("concrete and pointer keys must be accepted")
	}
	if StrictType[any]() || StrictType[nested]() || StrictType[[1]any]() {
		t.Fatal("interface-bearing keys must be rejected")
	}
}

func TestReflexiveTypeRecognizesTypesThatNeedValueValidation(t *testing.T) {
	type nested struct{ Value [2]complex128 }
	type blank struct {
		ID int
		_  float64
	}
	if !ReflexiveType[string]() || !ReflexiveType[int]() || !ReflexiveType[*nested]() || !ReflexiveType[blank]() || !ReflexiveType[[0]float64]() {
		t.Fatal("every value of these types is a reflexive key")
	}
	if ReflexiveType[float32]() || ReflexiveType[float64]() || ReflexiveType[complex64]() || ReflexiveType[complex128]() || ReflexiveType[nested]() || ReflexiveType[any]() || ReflexiveType[[1]any]() {
		t.Fatal("a type with invalid possible values cannot bypass value checks")
	}
}

func TestValueChecksComparabilityAndReflexivity(t *testing.T) {
	type nested struct{ Value any }
	nan := math.NaN()
	for _, value := range []any{nil, "safe", 12, math.Inf(1), complex(1, 2), nested{Value: 1}, &nan, [0]float64{}} {
		if !Value(value) {
			t.Errorf("valid key rejected: %#v", value)
		}
	}
	for _, value := range []any{[]int{1}, map[string]int{}, func() {}, nan, float32(nan), complex(nan, 0), complex(0, nan), [1]float64{nan}, nested{Value: []int{1}}, nested{Value: complex(0, nan)}} {
		if Value(value) {
			t.Errorf("invalid key accepted: %#v", value)
		}
	}
}

func FuzzValueFloatingKeys(f *testing.F) {
	f.Add(uint64(0), uint64(0x7ff8000000000001))
	f.Add(uint64(0x7ff0000000000000), uint64(0x8000000000000000))
	f.Fuzz(func(t *testing.T, realBits, imaginaryBits uint64) {
		realPart, imaginaryPart := math.Float64frombits(realBits), math.Float64frombits(imaginaryBits)
		key := struct{ Value [1]complex128 }{[1]complex128{complex(realPart, imaginaryPart)}}
		want := !math.IsNaN(realPart) && !math.IsNaN(imaginaryPart)
		if got := Value(key); got != want {
			t.Fatalf("Value(%v) = %v, want %v", key, got, want)
		}
		if got := Value[any](key); got != want {
			t.Fatalf("dynamic Value(%v) = %v, want %v", key, got, want)
		}
	})
}
