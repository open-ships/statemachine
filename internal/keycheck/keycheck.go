// Package keycheck validates generic values before they are used as map keys.
package keycheck

import "reflect"

// StrictType reports whether T can be used without a run-time comparability
// panic. Go's comparable constraint admits interfaces, including interfaces
// nested in structs and arrays, whose dynamic values may be uncomparable.
func StrictType[T comparable]() bool {
	return strict(reflect.TypeFor[T]())
}

// ReflexiveType reports whether every value of T is a safe, reflexive map key.
// Floating-point and complex values can contain NaN; interfaces can contain
// either NaN or uncomparable values. Pointers compare by identity, regardless
// of the type or contents of the value they point to.
func ReflexiveType[T comparable]() bool {
	return reflexive(reflect.TypeFor[T]())
}

func reflexive(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface, reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return false
	case reflect.Array:
		return t.Len() == 0 || reflexive(t.Elem())
	case reflect.Struct:
		for index := 0; index < t.NumField(); index++ {
			field := t.Field(index)
			if field.Name != "_" && !reflexive(field.Type) {
				return false
			}
		}
	}
	return true
}

func strict(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Interface:
		return false
	case reflect.Array:
		return strict(t.Elem())
	case reflect.Struct:
		for index := 0; index < t.NumField(); index++ {
			if !strict(t.Field(index).Type) {
				return false
			}
		}
	}
	return true
}

// Value reports whether value is comparable and equal to itself. A NaN-bearing
// key can be inserted into a Go map but can never be retrieved, so comparability
// alone is insufficient. The comparability check must precede equality because
// interface-bearing values can otherwise panic during the comparison.
func Value[T comparable](value T) bool {
	return reflect.ValueOf(&value).Elem().Comparable() && value == value
}
