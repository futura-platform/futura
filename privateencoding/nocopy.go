package privateencoding

import (
	"reflect"
	"sync"
)

// noCopyTypes are the standard library's no-copy types: their fields are runtime state, not logical
// state, so a value of one of these types is skipped (encoded as nothing, decoded as zero). Only the
// type itself is skipped; a struct that embeds one still encodes its own fields.
//
// The no-copy property cannot be detected by reflection, so this is a list of the known ones, not a
// rule about the sync package: sync.Map holds user data and must not be skipped.
var noCopyTypes = map[reflect.Type]struct{}{
	reflect.TypeOf(sync.Mutex{}):     {},
	reflect.TypeOf(sync.RWMutex{}):   {},
	reflect.TypeOf(sync.Once{}):      {},
	reflect.TypeOf(sync.WaitGroup{}): {},
	reflect.TypeOf(sync.Cond{}):      {},
}

func isNoCopyType(t reflect.Type) bool {
	_, ok := noCopyTypes[t]
	return ok
}
