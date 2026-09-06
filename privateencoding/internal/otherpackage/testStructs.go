package otherpackage

import "reflect"

type UnsafeValueTest struct {
	ExportedField   int
	unexportedField int
}

func NewMyStruct(ExportedField int, UnexportedField int) *UnsafeValueTest {
	return &UnsafeValueTest{ExportedField: ExportedField, unexportedField: UnexportedField}
}

type CodecTest[T any] struct {
	Direct, iDirect           T
	Pointer, iPointer         *T
	Slice, iSlice             []T
	Map, iMap                 map[any]T
	StructField, iStructField struct{ Field T }
}

func NewCodecTestStruct[T any](value T) *CodecTest[T] {
	return &CodecTest[T]{
		Direct:       value,
		iDirect:      value,
		Pointer:      &value,
		iPointer:     &value,
		Slice:        []T{value},
		iSlice:       []T{value},
		Map:          map[any]T{"key": value},
		iMap:         map[any]T{"key": value},
		StructField:  struct{ Field T }{Field: value},
		iStructField: struct{ Field T }{Field: value},
	}
}

// UnexportedFieldType is an unnamed struct whose unexported field makes this package part of its identity.
func UnexportedFieldType() reflect.Type { return reflect.TypeOf(struct{ id int }{}) }

// UnexportedMethodType is an unnamed interface whose unexported method makes this package part of its identity.
func UnexportedMethodType() reflect.Type { return reflect.TypeFor[interface{ id() }]() }
