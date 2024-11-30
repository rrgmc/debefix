package debefix

import (
	"iter"
	"maps"
)

// Values represents a list of the row fields and values, similar to a map[string]any.
type Values interface {
	Get(fieldName string) (val any, exists bool) // gets the value of a field, returning whether the field exists.
	GetOrNil(fieldName string) any               // gets the value of a field, or nil if the field don't exist.
	GetDefault(fieldName string, def any) any    // gets the value of a field, or a default value if the field don't exist.
	All(yield func(string, any) bool)            // iterator of all the field values.
	Len() int                                    // returns the amount of field values.
}

// ValuesMutable is a Values that can be mutated.
type ValuesMutable interface {
	Values
	Set(fieldName string, val any)     // sets a field value.
	Insert(seq iter.Seq2[string, any]) // insert a list of field values.
	Delete(fieldName ...string)        // delete field values.
}

// ValuesGet gets a value from values casting to the T type.
func ValuesGet[T any](values Values, fieldName string) (val T, exists bool, isType bool) {
	v, ok := values.Get(fieldName)
	if !ok {
		var ret T
		return ret, ok, false
	}
	vt, ok := v.(T)
	return vt, true, ok
}

// NewValues creates a new ValuesMutable.
func NewValues(val map[string]any) ValuesMutable {
	if val == nil {
		val = make(map[string]any)
	}
	return MapValues(val)
}

// MapValues is a ValuesMutable implementation using a map[string]any
type MapValues map[string]any

func (v MapValues) Get(fieldName string) (val any, exists bool) {
	val, exists = v[fieldName]
	return
}

func (v MapValues) GetDefault(fieldName string, def any) any {
	if val, ok := v[fieldName]; ok {
		return val
	}
	return def
}

func (v MapValues) GetOrNil(fieldName string) any {
	return v[fieldName]
}

func (v MapValues) Len() int {
	return len(v)
}

func (v MapValues) All(yield func(string, any) bool) {
	maps.All(v)(yield)
}

func (v MapValues) Insert(seq iter.Seq2[string, any]) {
	maps.Insert(v, seq)
}

func (v MapValues) Set(fieldName string, val any) {
	v[fieldName] = val
}

func (v MapValues) Delete(fieldName ...string) {
	for _, fieldName := range fieldName {
		delete(v, fieldName)
	}
}
