package privateencoding

import "encoding/binary"

func endianness() binary.ByteOrder {
	return binary.LittleEndian
}
