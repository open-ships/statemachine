// Package keycheck validates generic values before they are used as map keys.
package keycheck

import "reflect"

// StrictType reports whether T can be used without a run-time comparability
// panic. Go's comparable constraint admits interfaces, including interfaces
// nested in structs and arrays, whose dynamic values may be uncomparable.
func StrictType[T comparable]() bool {
	return strict(reflect.TypeFor[T]())
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

// Value reports whether value's current dynamic representation is comparable.
// It is useful at interfaces that cannot reject an unsafe generic type during
// construction, such as the zero Machine and Store adapters.
func Value[T comparable](value T) bool {
	return reflect.ValueOf(&value).Elem().Comparable()
}
