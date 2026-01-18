package pgdump

import (
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

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
	OidMacaddr     = 829
	OidInet        = 869
	OidDate        = 1082
	OidTime        = 1083
	OidTimestamp   = 1114
	OidTimestampTZ = 1184
	OidInterval    = 1186
	OidVarchar     = 1043
	OidNumeric     = 1700
	OidUUID        = 2950
	OidJSONB       = 3802
)

var (
	pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	typeNames = map[int]string{
		OidBool: "bool", OidBytea: "bytea", OidChar: "char", OidName: "name",
		OidInt8: "int8", OidInt2: "int2", OidInt4: "int4", OidText: "text",
		OidOid: "oid", OidJSON: "json", OidFloat4: "float4", OidFloat8: "float8",
		OidInet: "inet", OidMacaddr: "macaddr", OidTime: "time", OidDate: "date",
		OidTimestamp: "timestamp", OidTimestampTZ: "timestamptz", OidInterval: "interval",
		OidVarchar: "varchar", OidUUID: "uuid", OidNumeric: "numeric", OidJSONB: "jsonb",
	}

	arrayElemTypes = map[int]int{
		1000: OidBool, 1005: OidInt2, 1007: OidInt4, 1016: OidInt8,
		1009: OidText, 1022: OidFloat8, 1015: OidVarchar, 3807: OidJSONB,
	}

	fixedLengths = map[int]int{
		OidBool: 1, OidChar: 1, OidInt2: 2, OidInt4: 4, OidInt8: 8, OidOid: 4,
		OidFloat4: 4, OidFloat8: 8, OidDate: 4, OidTimestamp: 8, OidTimestampTZ: 8,
	}
)

// TypeName returns human-readable type name
func TypeName(oid int) string {
	if name, ok := typeNames[oid]; ok {
		return name
	}
	return fmt.Sprintf("oid:%d", oid)
}

// DecodeType decodes PostgreSQL binary data to Go value
func DecodeType(data []byte, oid int) interface{} {
	if len(data) == 0 {
		return nil
	}
	if elemOid, ok := arrayElemTypes[oid]; ok {
		return decodeArray(data, elemOid)
	}
	return decodeScalar(data, oid)
}

func decodeScalar(data []byte, oid int) interface{} {
	switch oid {
	case OidBool:
		return data[0] != 0
	case OidChar:
		return string(data[:1])
	case OidName:
		return cstring(data, 64)
	case OidInt2:
		return i16(data, 0)
	case OidInt4:
		return i32(data, 0)
	case OidInt8:
		return i64(data, 0)
	case OidOid:
		return u32(data, 0)
	case OidFloat4:
		return math.Float32frombits(u32(data, 0))
	case OidFloat8:
		return math.Float64frombits(u64(data, 0))
	case OidText, OidVarchar, OidJSON:
		return safeString(data)
	case OidBytea:
		return fmt.Sprintf("\\x%x", data)
	case OidDate:
		return pgEpoch.AddDate(0, 0, int(i32(data, 0))).Format("2006-01-02")
	case OidTime:
		us := i64(data, 0)
		return fmt.Sprintf("%02d:%02d:%02d", us/3600e6, (us/60e6)%60, (us/1e6)%60)
	case OidTimestamp, OidTimestampTZ:
		return pgEpoch.Add(time.Duration(i64(data, 0)) * time.Microsecond).Format("2006-01-02 15:04:05")
	case OidInterval:
		return decodeInterval(data)
	case OidMacaddr:
		return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", data[0], data[1], data[2], data[3], data[4], data[5])
	case OidInet:
		return decodeInet(data)
	case OidUUID:
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%x", u32(data, 0), u16(data, 4), u16(data, 6), u16(data, 8), data[10:16])
	case OidNumeric:
		return DecodeNumeric(data)
	case OidJSONB:
		if v := ParseJSONB(data); v != nil {
			return v
		}
		return safeString(data)
	default:
		return safeString(data)
	}
}

func decodeInterval(data []byte) string {
	if len(data) < 16 {
		return "0"
	}
	us, days, months := i64(data, 0), i32(data, 8), i32(data, 12)
	var parts []string
	if y := months / 12; y > 0 {
		parts = append(parts, fmt.Sprintf("%dy", y))
	}
	if m := months % 12; m > 0 {
		parts = append(parts, fmt.Sprintf("%dmo", m))
	}
	if days > 0 {
		parts = append(parts, fmt.Sprintf("%dd", days))
	}
	if h := us / 3600e6; h > 0 {
		parts = append(parts, fmt.Sprintf("%dh", h))
	}
	if len(parts) == 0 {
		return "0"
	}
	return strings.Join(parts, " ")
}

func decodeInet(data []byte) string {
	if len(data) < 2 {
		return ""
	}
	if data[0] == 2 && len(data) >= 6 {
		return fmt.Sprintf("%d.%d.%d.%d", data[2], data[3], data[4], data[5])
	}
	if data[0] == 3 && len(data) >= 18 {
		var p []string
		for i := 0; i < 8; i++ {
			p = append(p, fmt.Sprintf("%x", u16(data, 2+i*2)))
		}
		return strings.Join(p, ":")
	}
	return fmt.Sprintf("inet:%x", data)
}

func decodeArray(raw []byte, elemOid int) []interface{} {
	if len(raw) < 20 {
		return nil
	}
	ndim := i32(raw, 0)
	if ndim <= 0 || ndim > 6 {
		return nil
	}

	dataoff := i32(raw, 4)
	total := int32(1)
	for i := int32(0); i < ndim; i++ {
		total *= i32(raw, 12+int(i)*4)
	}
	if total <= 0 {
		return nil
	}

	var nullBitmap []byte
	dataStart := 12 + ndim*8
	if dataoff > 0 {
		nullBitmap = raw[dataStart : dataStart+(total+7)/8]
		dataStart = dataoff
	}

	elemLen, fixed := fixedLengths[elemOid]
	return parseArrayElements(raw, int(dataStart), int(total), elemOid, elemLen, fixed, nullBitmap)
}

func parseArrayElements(raw []byte, off, count, elemOid, elemLen int, fixed bool, nulls []byte) []interface{} {
	elems := make([]interface{}, 0, count)
	for i := 0; i < count; i++ {
		if nulls != nil && nulls[i/8]&(1<<(i%8)) == 0 {
			elems = append(elems, nil)
			continue
		}
		if fixed {
			elems = append(elems, DecodeType(raw[off:off+elemLen], elemOid))
			off += elemLen
		} else {
			if i > 0 {
				off = align(off, 4)
			}
			if off >= len(raw) {
				break
			}
			if hdr := raw[off]; hdr&1 == 1 {
				n := int(hdr >> 1)
				elems = append(elems, DecodeType(raw[off+1:off+n], elemOid))
				off += n
			} else {
				n := int(u32(raw, off) >> 2)
				elems = append(elems, DecodeType(raw[off+4:off+n], elemOid))
				off += n
			}
		}
	}
	return elems
}

// ReadVarlena reads a varlena value, returning data and bytes consumed
func ReadVarlena(data []byte) ([]byte, int) {
	if len(data) == 0 {
		return nil, 0
	}

	// Skip padding
	pad := 0
	for pad < len(data) && data[pad] == 0 {
		pad++
	}
	if pad >= len(data) {
		return nil, pad
	}

	first := data[pad]
	switch {
	case first&1 == 0: // Long varlena
		if len(data) < pad+4 {
			return nil, pad
		}
		n := int(u32(data, pad)>>2) - 4
		if n >= 0 && len(data) >= pad+4+n {
			return data[pad+4 : pad+4+n], pad + 4 + n
		}
	case first == 1: // NULL toast
		return nil, pad + 1
	default: // Short varlena
		n := int(first>>1) - 1
		if n >= 0 && len(data) >= pad+1+n {
			return data[pad+1 : pad+1+n], pad + 1 + n
		}
	}
	return nil, pad
}

func safeString(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	return strings.ToValidUTF8(string(data), ".")
}
