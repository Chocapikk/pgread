package pgdump

import (
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// PostgreSQL type OIDs (from pg_type.dat)
const (
	// Basic types
	OidBool   = 16
	OidBytea  = 17
	OidChar   = 18
	OidName   = 19
	OidInt8   = 20
	OidInt2   = 21
	OidInt4   = 23
	OidText   = 25
	OidOid    = 26
	OidTid    = 27
	OidXid    = 28
	OidCid    = 29
	OidJSON   = 114
	OidXML    = 142

	// Geometric types
	OidPoint   = 600
	OidLseg    = 601
	OidPath    = 602
	OidBox     = 603
	OidPolygon = 604
	OidLine    = 628
	OidCircle  = 718

	// Network types
	OidCidr     = 650
	OidFloat4   = 700
	OidFloat8   = 701
	OidMacaddr8 = 774
	OidMoney    = 790
	OidMacaddr  = 829
	OidInet     = 869

	// String types
	OidBpchar  = 1042
	OidVarchar = 1043

	// Date/Time types
	OidDate        = 1082
	OidTime        = 1083
	OidTimestamp   = 1114
	OidTimestampTZ = 1184
	OidInterval    = 1186
	OidTimeTZ      = 1266

	// Bit types
	OidBit    = 1560
	OidVarbit = 1562

	// Numeric
	OidNumeric = 1700

	// UUID
	OidUUID = 2950

	// pg_lsn
	OidPgLsn = 3220

	// Text search
	OidTsvector = 3614
	OidTsquery  = 3615

	// JSON
	OidJSONB    = 3802
	OidJSONPath = 4072

	// Range types
	OidInt4Range  = 3904
	OidNumRange   = 3906
	OidTsRange    = 3908
	OidTsTzRange  = 3910
	OidDateRange  = 3912
	OidInt8Range  = 3926
)

var (
	pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	typeNames = map[int]string{
		OidBool: "bool", OidBytea: "bytea", OidChar: "char", OidName: "name",
		OidInt8: "int8", OidInt2: "int2", OidInt4: "int4", OidText: "text",
		OidOid: "oid", OidTid: "tid", OidXid: "xid", OidCid: "cid",
		OidJSON: "json", OidXML: "xml",
		OidPoint: "point", OidLseg: "lseg", OidPath: "path", OidBox: "box",
		OidPolygon: "polygon", OidLine: "line", OidCircle: "circle",
		OidCidr: "cidr", OidFloat4: "float4", OidFloat8: "float8",
		OidMacaddr8: "macaddr8", OidMoney: "money", OidMacaddr: "macaddr", OidInet: "inet",
		OidBpchar: "bpchar", OidVarchar: "varchar",
		OidDate: "date", OidTime: "time", OidTimestamp: "timestamp",
		OidTimestampTZ: "timestamptz", OidInterval: "interval", OidTimeTZ: "timetz",
		OidBit: "bit", OidVarbit: "varbit",
		OidNumeric: "numeric", OidUUID: "uuid", OidPgLsn: "pg_lsn",
		OidTsvector: "tsvector", OidTsquery: "tsquery",
		OidJSONB: "jsonb", OidJSONPath: "jsonpath",
		OidInt4Range: "int4range", OidNumRange: "numrange", OidTsRange: "tsrange",
		OidTsTzRange: "tstzrange", OidDateRange: "daterange", OidInt8Range: "int8range",
	}

	arrayElemTypes = map[int]int{
		1000: OidBool, 1001: OidBytea, 1002: OidChar, 1003: OidName,
		1005: OidInt2, 1006: OidInt2, 1007: OidInt4, 1008: OidOid,
		1009: OidText, 1010: OidTid, 1011: OidXid, 1012: OidCid,
		1014: OidBpchar, 1015: OidVarchar, 1016: OidInt8,
		1017: OidPoint, 1018: OidLseg, 1019: OidPath, 1020: OidBox,
		1021: OidFloat4, 1022: OidFloat8, 1027: OidPolygon,
		1028: OidOid, 1040: OidMacaddr, 1041: OidInet,
		1115: OidTimestamp, 1182: OidDate, 1183: OidTime,
		1185: OidTimestampTZ, 1187: OidInterval, 1231: OidNumeric,
		1270: OidTimeTZ, 1561: OidBit, 1563: OidVarbit,
		2951: OidUUID, 3221: OidPgLsn, 3643: OidTsvector, 3645: OidTsquery,
		3807: OidJSONB, 4073: OidJSONPath,
		629: OidLine, 651: OidCidr, 719: OidCircle, 775: OidMacaddr8, 791: OidMoney,
		3905: OidInt4Range, 3907: OidNumRange, 3909: OidTsRange,
		3911: OidTsTzRange, 3913: OidDateRange, 3927: OidInt8Range,
	}

	fixedLengths = map[int]int{
		OidBool: 1, OidChar: 1, OidInt2: 2, OidInt4: 4, OidInt8: 8, OidOid: 4,
		OidFloat4: 4, OidFloat8: 8, OidDate: 4, OidTimestamp: 8, OidTimestampTZ: 8,
		OidTid: 6, OidXid: 4, OidCid: 4, OidMoney: 8, OidTime: 8,
		OidMacaddr: 6, OidMacaddr8: 8, OidUUID: 16, OidPgLsn: 8,
		OidPoint: 16, OidLseg: 32, OidBox: 32, OidLine: 24, OidCircle: 24,
		OidTimeTZ: 12, OidInterval: 16,
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
	// Boolean
	case OidBool:
		return data[0] != 0

	// Characters
	case OidChar:
		return string(data[:1])
	case OidName:
		return cstring(data, 64)

	// Integers
	case OidInt2:
		return i16(data, 0)
	case OidInt4, OidXid, OidCid:
		return i32(data, 0)
	case OidInt8:
		return i64(data, 0)
	case OidOid:
		return u32(data, 0)
	case OidTid:
		return fmt.Sprintf("(%d,%d)", u32(data, 0), u16(data, 4))

	// Floats
	case OidFloat4:
		return math.Float32frombits(u32(data, 0))
	case OidFloat8:
		return math.Float64frombits(u64(data, 0))

	// Money (int64 in cents)
	case OidMoney:
		cents := i64(data, 0)
		return fmt.Sprintf("$%.2f", float64(cents)/100)

	// Text types
	case OidText, OidVarchar, OidBpchar, OidJSON, OidXML, OidJSONPath:
		return safeString(data)
	case OidBytea:
		return fmt.Sprintf("\\x%x", data)

	// Bit strings
	case OidBit, OidVarbit:
		return decodeBitString(data)

	// Date/Time
	case OidDate:
		return pgEpoch.AddDate(0, 0, int(i32(data, 0))).Format("2006-01-02")
	case OidTime:
		us := i64(data, 0)
		return fmt.Sprintf("%02d:%02d:%02d", us/3600e6, (us/60e6)%60, (us/1e6)%60)
	case OidTimeTZ:
		us := i64(data, 0)
		tz := i32(data, 8) // timezone offset in seconds
		return fmt.Sprintf("%02d:%02d:%02d%+03d", us/3600e6, (us/60e6)%60, (us/1e6)%60, -tz/3600)
	case OidTimestamp, OidTimestampTZ:
		return pgEpoch.Add(time.Duration(i64(data, 0)) * time.Microsecond).Format("2006-01-02 15:04:05")
	case OidInterval:
		return decodeInterval(data)

	// Network
	case OidMacaddr:
		return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", data[0], data[1], data[2], data[3], data[4], data[5])
	case OidMacaddr8:
		return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x", data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7])
	case OidInet, OidCidr:
		return decodeInet(data)

	// UUID
	case OidUUID:
		return fmt.Sprintf("%08x-%04x-%04x-%04x-%x", u32(data, 0), u16(data, 4), u16(data, 6), u16(data, 8), data[10:16])

	// pg_lsn
	case OidPgLsn:
		return fmt.Sprintf("%X/%X", u32(data, 0), u32(data, 4))

	// Geometric
	case OidPoint:
		return decodePoint(data)
	case OidLseg:
		return fmt.Sprintf("[%s,%s]", decodePoint(data[0:16]), decodePoint(data[16:32]))
	case OidBox:
		return fmt.Sprintf("(%s),(%s)", decodePoint(data[0:16]), decodePoint(data[16:32]))
	case OidLine:
		return fmt.Sprintf("{%g,%g,%g}", math.Float64frombits(u64(data, 0)), math.Float64frombits(u64(data, 8)), math.Float64frombits(u64(data, 16)))
	case OidCircle:
		return fmt.Sprintf("<%s,%g>", decodePoint(data[0:16]), math.Float64frombits(u64(data, 16)))
	case OidPath, OidPolygon:
		return decodePathOrPolygon(data, oid)

	// Numeric
	case OidNumeric:
		return DecodeNumeric(data)

	// Text search (return as string for now)
	case OidTsvector, OidTsquery:
		return safeString(data)

	// JSON
	case OidJSONB:
		if v := ParseJSONB(data); v != nil {
			return v
		}
		return safeString(data)

	// Range types
	case OidInt4Range, OidInt8Range, OidNumRange, OidTsRange, OidTsTzRange, OidDateRange:
		return decodeRange(data, oid)

	default:
		return safeString(data)
	}
}

func decodePoint(data []byte) string {
	if len(data) < 16 {
		return "(?,?)"
	}
	x := math.Float64frombits(u64(data, 0))
	y := math.Float64frombits(u64(data, 8))
	return fmt.Sprintf("(%g,%g)", x, y)
}

func decodePathOrPolygon(data []byte, oid int) string {
	if len(data) < 5 {
		return ""
	}
	closed := data[0] != 0
	npts := int(i32(data, 1))
	if len(data) < 5+npts*16 {
		return ""
	}

	points := make([]string, npts)
	for i := 0; i < npts; i++ {
		points[i] = decodePoint(data[5+i*16 : 5+(i+1)*16])
	}

	joined := strings.Join(points, ",")
	if oid == OidPolygon || closed {
		return "(" + joined + ")"
	}
	return "[" + joined + "]"
}

func decodeBitString(data []byte) string {
	if len(data) < 4 {
		return ""
	}
	bitlen := int(i32(data, 0))
	if bitlen == 0 {
		return ""
	}

	var sb strings.Builder
	for i := 0; i < bitlen; i++ {
		byteIdx := 4 + i/8
		bitIdx := 7 - (i % 8)
		if byteIdx < len(data) && (data[byteIdx]&(1<<bitIdx)) != 0 {
			sb.WriteByte('1')
		} else {
			sb.WriteByte('0')
		}
	}
	return sb.String()
}

func decodeRange(data []byte, oid int) string {
	// PostgreSQL range format (after varlena header):
	// - range type OID: 4 bytes
	// - lower bound (if present): fixed size based on element type
	// - upper bound (if present): fixed size, aligned
	// - flags: 1 byte AT THE END
	
	if len(data) < 5 { // minimum: 4 bytes OID + 1 byte flags
		return "empty"
	}

	// Flags are stored at the LAST byte
	flags := data[len(data)-1]
	
	const (
		rangeEmpty = 0x01
		rangeLbInc = 0x02
		rangeUbInc = 0x04
		rangeLbInf = 0x08
		rangeUbInf = 0x10
	)

	if flags&rangeEmpty != 0 {
		return "empty"
	}

	lbInc := flags&rangeLbInc != 0
	ubInc := flags&rangeUbInc != 0
	lbInf := flags&rangeLbInf != 0
	ubInf := flags&rangeUbInf != 0

	// Determine element type and size
	var elemOid, elemSize int
	switch oid {
	case OidInt4Range:
		elemOid, elemSize = OidInt4, 4
	case OidInt8Range:
		elemOid, elemSize = OidInt8, 8
	case OidDateRange:
		elemOid, elemSize = OidDate, 4
	case OidTsRange:
		elemOid, elemSize = OidTimestamp, 8
	case OidTsTzRange:
		elemOid, elemSize = OidTimestampTZ, 8
	case OidNumRange:
		// Numeric is variable length, handle separately
		return decodeNumericRange(data, flags)
	default:
		return fmt.Sprintf("range:%x", data)
	}

	// Skip the range type OID (4 bytes)
	offset := 4
	dataEnd := len(data) - 1 // exclude flags byte

	var lb, ub string

	// Read lower bound if present
	if !lbInf {
		if offset+elemSize > dataEnd {
			return "[?,?]"
		}
		lb = fmt.Sprintf("%v", DecodeType(data[offset:offset+elemSize], elemOid))
		offset += elemSize
	}

	// Read upper bound if present (may need alignment)
	if !ubInf {
		// Align offset for upper bound
		if elemSize > 1 {
			offset = align(offset, elemSize)
		}
		if offset+elemSize > dataEnd {
			return "[?,?]"
		}
		ub = fmt.Sprintf("%v", DecodeType(data[offset:offset+elemSize], elemOid))
	}

	// Format output
	var result strings.Builder
	if lbInc {
		result.WriteByte('[')
	} else {
		result.WriteByte('(')
	}
	if lbInf {
		result.WriteByte(',')
	} else {
		result.WriteString(lb)
		result.WriteByte(',')
	}
	if !ubInf {
		result.WriteString(ub)
	}
	if ubInc {
		result.WriteByte(']')
	} else {
		result.WriteByte(')')
	}

	return result.String()
}

func decodeNumericRange(data []byte, flags byte) string {
	// Numeric ranges have variable-length bounds, more complex to parse
	// For now, return a simplified representation
	const (
		rangeLbInc = 0x02
		rangeUbInc = 0x04
		rangeLbInf = 0x08
		rangeUbInf = 0x10
	)

	lbInc := flags&rangeLbInc != 0
	ubInc := flags&rangeUbInc != 0
	lbInf := flags&rangeLbInf != 0
	ubInf := flags&rangeUbInf != 0

	var result strings.Builder
	if lbInc {
		result.WriteByte('[')
	} else {
		result.WriteByte('(')
	}
	if lbInf {
		result.WriteString(",")
	} else {
		result.WriteString("?,")
	}
	if ubInf {
		// nothing
	} else {
		result.WriteString("?")
	}
	if ubInc {
		result.WriteByte(']')
	} else {
		result.WriteByte(')')
	}
	return result.String()
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
	if m := (us / 60e6) % 60; m > 0 {
		parts = append(parts, fmt.Sprintf("%dm", m))
	}
	if s := (us / 1e6) % 60; s > 0 {
		parts = append(parts, fmt.Sprintf("%ds", s))
	}
	if len(parts) == 0 {
		return "0"
	}
	return strings.Join(parts, " ")
}

func decodeInet(data []byte) string {
	if len(data) < 4 {
		return ""
	}
	family := data[0]
	bits := data[1]
	// isCidr := data[2] != 0
	addrLen := data[3]

	if family == 2 && addrLen == 4 && len(data) >= 8 {
		// IPv4
		addr := fmt.Sprintf("%d.%d.%d.%d", data[4], data[5], data[6], data[7])
		if bits != 32 {
			return fmt.Sprintf("%s/%d", addr, bits)
		}
		return addr
	}
	if family == 3 && addrLen == 16 && len(data) >= 20 {
		// IPv6
		var parts []string
		for i := 0; i < 8; i++ {
			parts = append(parts, fmt.Sprintf("%x", binary.BigEndian.Uint16(data[4+i*2:6+i*2])))
		}
		addr := strings.Join(parts, ":")
		if bits != 128 {
			return fmt.Sprintf("%s/%d", addr, bits)
		}
		return addr
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
			if off+elemLen > len(raw) {
				break
			}
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
				if off+4 > len(raw) {
					break
				}
				n := int(u32(raw, off) >> 2)
				elems = append(elems, DecodeType(raw[off+4:off+n], elemOid))
				off += n
			}
		}
	}
	return elems
}

// ReadVarlena reads a varlena value, returning data and bytes consumed
// PostgreSQL varlena format:
// - Short varlena: first byte has bit0=1, length = (first_byte >> 1), includes header
// - Long varlena: 4-byte header with bit0=0, length = (header >> 2), includes header
// - TOAST pointer: first byte = 0x01 (external storage)
func ReadVarlena(data []byte) ([]byte, int) {
	if len(data) == 0 {
		return nil, 0
	}

	first := data[0]
	
	// Check for short varlena (1-byte header, bit 0 set but not just 0x01)
	if first&1 == 1 && first != 1 {
		totalLen := int(first >> 1)
		if totalLen <= 1 || len(data) < totalLen {
			return nil, 1
		}
		return data[1:totalLen], totalLen
	}
	
	// Check for TOAST pointer (external storage, treat as null)
	if first == 1 {
		return nil, 1
	}
	
	// Long varlena (4-byte header)
	if len(data) < 4 {
		return nil, 0
	}
	header := u32(data, 0)
	totalLen := int(header >> 2)
	if totalLen < 4 || len(data) < totalLen {
		return nil, 4
	}
	return data[4:totalLen], totalLen
}

func safeString(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	return strings.ToValidUTF8(string(data), ".")
}
