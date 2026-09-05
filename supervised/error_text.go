package supervised

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// cachedError keeps diagnostic rendering separate from error identity. Its
// original cause remains reachable through errors.Is and errors.As.
type cachedError struct {
	cause error
	text  string
}

func (e *cachedError) Error() string { return e.text }
func (e *cachedError) Unwrap() error { return e.cause }

// captureErrorText runs only inside an already-owned callback goroutine. If an
// application's Error method blocks or exits, the callback's existing deadline
// and ownership protocol still apply. A formatting panic retains the original
// error with safe fallback text. No additional goroutine is started.
func captureErrorText(err error) (captured error) {
	if err == nil {
		return nil
	}
	if _, already := err.(*cachedError); already {
		return err
	}
	cached := &cachedError{cause: err, text: safeErrorText(err)}
	captured = cached
	defer func() { _ = recover() }()
	budget := errorTextBudget{remaining: 64, capture: true}
	cached.text = budget.text(err)
	return captured
}

var (
	plainErrorType    = reflect.TypeOf(errors.New(""))
	wrappedErrorType  = reflect.TypeOf(fmt.Errorf("%w", errors.New("")))
	wrappedErrorsType = reflect.TypeOf(fmt.Errorf("%w %w", errors.New(""), errors.New("")))
	joinedErrorType   = reflect.TypeOf(errors.Join(errors.New("")))
	deadlineErrorType = reflect.TypeOf(context.DeadlineExceeded)
)

// safeErrorText never calls application Error, String, Format, or Unwrap
// methods. Exact standard-library error types have precomputed or constant
// text; Supervisor errors render stored fields. Everything else uses its type
// name until captureErrorText can render it inside an owned callback.
func safeErrorText(err error) (text string) {
	if err == nil {
		return "<nil>"
	}
	// Applications can promote a library renderer through an embedded nil
	// pointer. Its generated method wrapper can panic before the renderer runs.
	text = "<" + reflect.TypeOf(err).String() + ": error text not captured>"
	defer func() { _ = recover() }()
	budget := errorTextBudget{remaining: 64}
	return budget.text(err)
}

type errorTextBudget struct {
	remaining int
	capture   bool
}

func (b *errorTextBudget) text(err error) string {
	if err == nil {
		return "<nil>"
	}
	if b.remaining == 0 {
		return "<error detail limit>"
	}
	b.remaining--
	value := reflect.ValueOf(err)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return "<nil>"
	}
	switch err := err.(type) {
	case *cachedError:
		return err.text
	case interface{ safeText(*errorTextBudget) string }:
		return err.safeText(b)
	}
	switch reflect.TypeOf(err) {
	case plainErrorType, wrappedErrorType, wrappedErrorsType, deadlineErrorType:
		return err.Error()
	case joinedErrorType:
		// Only the concrete errors.Join result reaches this assertion; arbitrary
		// application Unwrap methods are never evaluated while rendering records.
		var text strings.Builder
		causes := err.(interface{ Unwrap() []error }).Unwrap()
		for index, cause := range causes {
			if index != 0 {
				text.WriteByte('\n')
			}
			text.WriteString(b.text(cause))
			if b.remaining == 0 {
				if index+1 < len(causes) {
					text.WriteString("\n<error detail limit>")
				}
				break
			}
		}
		return text.String()
	default:
		if b.capture {
			return err.Error()
		}
		return "<" + reflect.TypeOf(err).String() + ": error text not captured>"
	}
}

// value renders primitive underlying values, never application Stringers.
// Composite values and pointers retain an informative type name.
func (b *errorTextBudget) value(value any) string {
	if value == nil {
		return "<nil>"
	}
	if err, ok := value.(error); ok {
		return b.text(err)
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.String:
		return reflected.String()
	case reflect.Bool:
		return strconv.FormatBool(reflected.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(reflected.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(reflected.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(reflected.Float(), 'g', -1, reflected.Type().Bits())
	case reflect.Complex64, reflect.Complex128:
		return strconv.FormatComplex(reflected.Complex(), 'g', -1, reflected.Type().Bits())
	default:
		return "<" + reflected.Type().String() + ">"
	}
}

func (f *Fault[S, E]) safeText(b *errorTextBudget) string {
	if f == nil {
		return "<nil>"
	}
	cause := f.CauseText
	if cause == "" {
		cause = b.text(f.Cause)
	}
	return fmt.Sprintf("supervised: fault in %s phase at state %s on event %s: %s", f.Phase, b.value(f.State), b.value(f.Event), cause)
}

func (e *PanicError) safeText(b *errorTextBudget) string {
	if e == nil {
		return "<nil>"
	}
	return "supervised: callback panicked: " + b.value(e.Value)
}

func (e *ViolationError) safeText(b *errorTextBudget) string {
	if e == nil {
		return "<nil>"
	}
	return "supervised: " + e.Phase.String() + ": " + b.text(e.Reason)
}

func (e *operationTimeoutError) safeText(b *errorTextBudget) string {
	if e == nil {
		return "<nil>"
	}
	return b.text(ErrOperationTimeout) + ": " + b.text(e.cause)
}

func (r *refusal[S, E]) safeText(b *errorTextBudget) string {
	if r == nil {
		return "<nil>"
	}
	message := "supervised: " + b.value(r.event) + " in state " + b.value(r.from) + ": " + b.text(ErrNotPermitted)
	if len(r.unwrap) > 1 {
		message += ": " + b.text(errors.Join(r.unwrap[1:]...))
	}
	return message
}
