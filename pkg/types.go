package pkg

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// PostgreSQL epoch: 2000-01-01 00:00:00 UTC
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

const usec = 1_000_000

// PostgreSQL type OIDs
const (
	OidBool        = 16
	OidBytea       = 17
	OidChar        = 18
	OidName        = 19
	OidInt8        = 20
	OidInt2        = 21
	OidInt4        = 23
	OidText        = 25
	OidOid         = 26
	OidJSON        = 114
	OidFloat4      = 700
	OidFloat8      = 701
	OidInet        = 869
	OidMacaddr     = 829
	OidTime        = 1083
	OidDate        = 1082
	OidTimestamp   = 1114
	OidTimestampTZ = 1184
	OidInterval    = 1186
	OidVarchar     = 1043
	OidUUID        = 2950
	OidNumeric     = 1700
	OidJSONB       = 3802
)

// Array type OIDs and their element types
var arrayElementTypes = map[int]int{
	1000: OidBool,
	1005: OidInt2,
	1007: OidInt4,
	1016: OidInt8,
	1009: OidText,
	1022: OidFloat8,
	1015: OidVarchar,
	3807: OidJSONB,
}

// Fixed type lengths for alignment
var fixedTypeLengths = map[int]int{
	OidBool: 1, OidChar: 1, OidInt2: 2, OidInt4: 4, OidInt8: 8, OidOid: 4,
	OidFloat4: 4, OidFloat8: 8, OidDate: 4, OidTimestamp: 8, OidTimestampTZ: 8,
}

// DecodeType decodes PostgreSQL binary data to Go value
func DecodeType(data []byte, oid int) interface{} {
	if data == nil || len(data) == 0 {
		return nil
	}

	// Check for array types
	if elemOid, ok := arrayElementTypes[oid]; ok {
		return decodeArray(data, elemOid)
	}

	return decodeScalar(data, oid)
}

func decodeScalar(data []byte, oid int) interface{} {
	switch oid {
	case OidBool:
		return data[0] != 0

	case OidBytea:
		return fmt.Sprintf("\\x%x", data)

	case OidChar:
		if len(data) > 0 {
			return string(data[0])
		}
		return ""

	case OidName:
		// Name is null-terminated, max 64 bytes
		idx := 0
		for idx < len(data) && idx < 64 && data[idx] != 0 {
			idx++
		}
		return string(data[:idx])

	case OidInt8:
		if len(data) >= 8 {
			return int64(binary.LittleEndian.Uint64(data[:8]))
		}
		return nil

	case OidInt2:
		if len(data) >= 2 {
			return int16(binary.LittleEndian.Uint16(data[:2]))
		}
		return nil

	case OidInt4:
		if len(data) >= 4 {
			return int32(binary.LittleEndian.Uint32(data[:4]))
		}
		return nil

	case OidText, OidVarchar:
		return safeString(data)

	case OidOid:
		if len(data) >= 4 {
			return binary.LittleEndian.Uint32(data[:4])
		}
		return nil

	case OidJSON:
		return safeString(data)

	case OidFloat4:
		if len(data) >= 4 {
			bits := binary.LittleEndian.Uint32(data[:4])
			return math.Float32frombits(bits)
		}
		return nil

	case OidFloat8:
		if len(data) >= 8 {
			bits := binary.LittleEndian.Uint64(data[:8])
			return math.Float64frombits(bits)
		}
		return nil

	case OidMacaddr:
		return decodeMacaddr(data)

	case OidInet:
		return decodeInet(data)

	case OidDate:
		return decodeDate(data)

	case OidTime:
		return decodeTime(data)

	case OidTimestamp, OidTimestampTZ:
		return decodeTimestamp(data)

	case OidInterval:
		return decodeInterval(data)

	case OidNumeric:
		return DecodeNumeric(data)

	case OidUUID:
		return decodeUUID(data)

	case OidJSONB:
		result := ParseJSONB(data)
		if result != nil {
			return result
		}
		return safeString(data)

	default:
		return safeString(data)
	}
}

func decodeMacaddr(data []byte) string {
	if len(data) < 6 {
		return ""
	}
	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x",
		data[0], data[1], data[2], data[3], data[4], data[5])
}

func decodeInet(data []byte) string {
	if len(data) < 2 {
		return ""
	}
	family := data[0]
	// bits := data[1]

	if family == 2 && len(data) >= 6 {
		// IPv4
		return fmt.Sprintf("%d.%d.%d.%d", data[2], data[3], data[4], data[5])
	} else if family == 3 && len(data) >= 18 {
		// IPv6
		var parts []string
		for i := 0; i < 8; i++ {
			w := binary.BigEndian.Uint16(data[2+i*2 : 4+i*2])
			parts = append(parts, fmt.Sprintf("%x", w))
		}
		return strings.Join(parts, ":")
	}
	return fmt.Sprintf("inet:%x", data)
}

func decodeDate(data []byte) string {
	if len(data) < 4 {
		return ""
	}
	days := int32(binary.LittleEndian.Uint32(data[:4]))
	t := pgEpoch.AddDate(0, 0, int(days))
	return t.Format("2006-01-02")
}

func decodeTime(data []byte) string {
	if len(data) < 8 {
		return ""
	}
	usecs := int64(binary.LittleEndian.Uint64(data[:8]))
	hours := usecs / (3600 * usec)
	mins := (usecs / (60 * usec)) % 60
	secs := (usecs / usec) % 60
	return fmt.Sprintf("%02d:%02d:%02d", hours, mins, secs)
}

func decodeTimestamp(data []byte) string {
	if len(data) < 8 {
		return ""
	}
	usecs := int64(binary.LittleEndian.Uint64(data[:8]))
	t := pgEpoch.Add(time.Duration(usecs) * time.Microsecond)
	return t.Format("2006-01-02 15:04:05")
}

func decodeInterval(data []byte) string {
	if len(data) < 16 {
		return "interval:?"
	}
	usecs := int64(binary.LittleEndian.Uint64(data[:8]))
	days := int32(binary.LittleEndian.Uint32(data[8:12]))
	months := int32(binary.LittleEndian.Uint32(data[12:16]))

	var parts []string
	if months >= 12 {
		parts = append(parts, fmt.Sprintf("%dy", months/12))
	}
	if months%12 > 0 {
		parts = append(parts, fmt.Sprintf("%dmo", months%12))
	}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if usecs >= 3600*usec {
		parts = append(parts, fmt.Sprintf("%dh", usecs/(3600*usec)))
	}
	if usecs >= 60*usec && (usecs/(60*usec))%60 > 0 {
		parts = append(parts, fmt.Sprintf("%dm", (usecs/(60*usec))%60))
	}
	if len(parts) == 0 {
		return "0"
	}
	return strings.Join(parts, " ")
}

func decodeUUID(data []byte) string {
	if len(data) < 16 {
		return ""
	}
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(data[0:4]),
		binary.BigEndian.Uint16(data[4:6]),
		binary.BigEndian.Uint16(data[6:8]),
		binary.BigEndian.Uint16(data[8:10]),
		data[10:16])
}

func decodeArray(raw []byte, elemOid int) []interface{} {
	if len(raw) < 20 {
		return nil
	}

	ndim := int32(binary.LittleEndian.Uint32(raw[0:4]))
	if ndim <= 0 || ndim > 6 {
		return nil
	}

	dataoffset := int32(binary.LittleEndian.Uint32(raw[4:8]))
	// elemType := binary.LittleEndian.Uint32(raw[8:12])

	// Read dimensions
	dims := make([]int32, ndim)
	for i := int32(0); i < ndim; i++ {
		dims[i] = int32(binary.LittleEndian.Uint32(raw[12+i*4 : 16+i*4]))
	}

	totalElements := int32(1)
	for _, d := range dims {
		totalElements *= d
	}
	if totalElements <= 0 {
		return nil
	}

	// Null bitmap if dataoffset != 0
	var nullBitmap []byte
	if dataoffset > 0 {
		bitmapSize := (totalElements + 7) / 8
		nullBitmap = raw[12+ndim*8 : 12+ndim*8+bitmapSize]
	}

	// Data starts after dims + lbounds (+ nullbitmap if present)
	dataStart := int32(12 + ndim*8)
	if dataoffset > 0 {
		dataStart = dataoffset
	}

	elemLen, hasFixedLen := fixedTypeLengths[elemOid]

	return parseArrayData(raw, int(dataStart), int(totalElements), elemOid, elemLen, hasFixedLen, nullBitmap)
}

func parseArrayData(raw []byte, offset, count, elemOid, elemLen int, hasFixedLen bool, nullBitmap []byte) []interface{} {
	elements := make([]interface{}, 0, count)

	for i := 0; i < count; i++ {
		if nullBitmap != nil && (nullBitmap[i/8]&(1<<(i%8))) == 0 {
			elements = append(elements, nil)
			continue
		}

		if hasFixedLen {
			if offset+elemLen > len(raw) {
				break
			}
			elements = append(elements, DecodeType(raw[offset:offset+elemLen], elemOid))
			offset += elemLen
		} else {
			// INTALIGN between varlena elements
			if i > 0 {
				offset = (offset + 3) &^ 3
			}
			if offset >= len(raw) {
				break
			}

			hdr := raw[offset]
			if (hdr & 1) == 1 {
				total := int(hdr >> 1)
				elements = append(elements, DecodeType(raw[offset+1:offset+total], elemOid))
				offset += total
			} else {
				if offset+4 > len(raw) {
					break
				}
				total := int(binary.LittleEndian.Uint32(raw[offset:offset+4]) >> 2)
				elements = append(elements, DecodeType(raw[offset+4:offset+total], elemOid))
				offset += total
			}
		}
	}

	return elements
}

// ReadVarlena reads a varlena value from data
func ReadVarlena(data []byte) ([]byte, int) {
	if len(data) == 0 {
		return nil, 0
	}

	// Skip padding bytes
	pad := 0
	for pad < len(data) && data[pad] == 0 {
		pad++
	}
	if pad >= len(data) {
		return nil, pad
	}

	first := data[pad]
	if (first & 0x01) == 0 {
		// Long varlena
		val, consumed := readLongVarlena(data[pad:])
		return val, pad + consumed
	} else if first == 0x01 {
		// NULL toast pointer
		return nil, pad + 1
	} else {
		// Short varlena
		length := int(first>>1) - 1
		if length >= 0 && len(data) >= pad+1+length {
			return data[pad+1 : pad+1+length], pad + 1 + length
		}
		return nil, pad
	}
}

func readLongVarlena(data []byte) ([]byte, int) {
	if len(data) < 4 {
		return nil, 0
	}
	length := int(binary.LittleEndian.Uint32(data[:4])>>2) - 4
	if length >= 0 && len(data) >= 4+length {
		return data[4 : 4+length], 4 + length
	}
	return nil, 0
}

func safeString(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	// Replace invalid UTF-8 with placeholder
	return strings.ToValidUTF8(string(data), ".")
}

// TypeName returns the name of a PostgreSQL type by OID
func TypeName(oid int) string {
	names := map[int]string{
		OidBool: "bool", OidBytea: "bytea", OidChar: "char", OidName: "name",
		OidInt8: "int8", OidInt2: "int2", OidInt4: "int4", OidText: "text",
		OidOid: "oid", OidJSON: "json", OidFloat4: "float4", OidFloat8: "float8",
		OidInet: "inet", OidMacaddr: "macaddr", OidTime: "time", OidDate: "date",
		OidTimestamp: "timestamp", OidTimestampTZ: "timestamptz", OidInterval: "interval",
		OidVarchar: "varchar", OidUUID: "uuid", OidNumeric: "numeric", OidJSONB: "jsonb",
	}
	if name, ok := names[oid]; ok {
		return name
	}
	return fmt.Sprintf("oid:%d", oid)
}
