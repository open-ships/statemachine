package keycheck

import "testing"

func TestStrictTypeRejectsInterfacesAtEveryKeyDepth(t *testing.T) {
	type nested struct{ Value any }
	if !StrictType[string]() || !StrictType[*nested]() {
		t.Fatal("concrete and pointer keys must be accepted")
	}
	if StrictType[any]() || StrictType[nested]() || StrictType[[1]any]() {
		t.Fatal("interface-bearing keys must be rejected")
	}
}

func TestValueChecksDynamicComparability(t *testing.T) {
	if !Value[any]("safe") || Value[any]([]int{1}) {
		t.Fatal("dynamic comparability was classified incorrectly")
	}
}
