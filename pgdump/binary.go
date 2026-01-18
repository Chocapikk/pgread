package pgdump

import (
	"encoding/binary"
)

// Binary reading helpers - DRY utilities for all parsers

func u16(data []byte, off int) uint16 {
	return binary.LittleEndian.Uint16(data[off:])
}

func u32(data []byte, off int) uint32 {
	return binary.LittleEndian.Uint32(data[off:])
}

func u64(data []byte, off int) uint64 {
	return binary.LittleEndian.Uint64(data[off:])
}

func i16(data []byte, off int) int16 {
	return int16(u16(data, off))
}

func i32(data []byte, off int) int32 {
	return int32(u32(data, off))
}

func i64(data []byte, off int) int64 {
	return int64(u64(data, off))
}

// align rounds offset up to alignment boundary
func align(offset, alignment int) int {
	if alignment <= 1 {
		return offset
	}
	return (offset + alignment - 1) &^ (alignment - 1)
}

// cstring reads null-terminated string
func cstring(data []byte, maxLen int) string {
	for i := 0; i < len(data) && i < maxLen; i++ {
		if data[i] == 0 {
			return string(data[:i])
		}
	}
	if maxLen < len(data) {
		return string(data[:maxLen])
	}
	return string(data)
}

// toInt converts various numeric types to int
func toInt(v interface{}) int {
	switch n := v.(type) {
	case int:
		return n
	case int16:
		return int(n)
	case int32:
		return int(n)
	case int64:
		return int(n)
	case uint32:
		return int(n)
	case uint16:
		return int(n)
	default:
		return 0
	}
}
