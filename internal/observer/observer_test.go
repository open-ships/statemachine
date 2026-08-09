package observer

import (
	"runtime"
	"strings"
	"testing"
)

func TestCallClassifiesCompletionPanicAndGoexit(t *testing.T) {
	if failure := Call(func() {}); failure != nil {
		t.Fatalf("normal Call = %+v", failure)
	}
	if failure := Call(func() { panic("observer panic") }); failure == nil || failure.Value != "observer panic" ||
		failure.Stopped || !strings.Contains(failure.Stack, "TestCallClassifiesCompletionPanicAndGoexit") {
		t.Fatalf("panic Call = %+v", failure)
	}
	if failure := Call(runtime.Goexit); failure == nil || !failure.Stopped || failure.Value != nil || failure.Stack == "" {
		t.Fatalf("Goexit Call = %+v", failure)
	}
}
