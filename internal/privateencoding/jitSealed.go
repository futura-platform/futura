package privateencoding

import (
	"encoding/gob"
	"reflect"

	mapset "github.com/deckarep/golang-set/v2"
)

var seenSubTypes = mapset.NewSet[string]()

func jitGobRegister(uv reflect.Value) {
	subType := uv.Type().String()
	if seenSubTypes.Contains(subType) {
		return
	}

	gob.Register(uv.Interface())
	seenSubTypes.Add(subType)
}
