package pgdump

import "math"

// JSONB constants
const (
	jbCMask     = 0x0FFFFFFF
	jbFObject   = 0x20000000
	jbFArray    = 0x40000000
	jbFScalar   = 0x10000000
	jeOffMask   = 0x0FFFFFFF
	jeHasOff    = 0x80000000
	jeString    = 0x00000000
	jeNumeric   = 0x10000000
	jeBoolFalse = 0x20000000
	jeBoolTrue  = 0x30000000
	jeNull      = 0x40000000
	jeContainer = 0x50000000
)

// ParseJSONB parses PostgreSQL JSONB binary format
func ParseJSONB(data []byte) interface{} {
	if len(data) < 4 {
		return nil
	}

	header := u32(data, 0)
	count := int(header & jbCMask)
	isObj, isArr := header&jbFObject != 0, header&jbFArray != 0

	if (!isObj && !isArr) || count <= 0 || count > 10000 {
		return nil
	}

	numEntries := count
	if isObj {
		numEntries *= 2
	}
	if 4+numEntries*4 > len(data) {
		return nil
	}

	entries := make([]uint32, numEntries)
	for i := range entries {
		entries[i] = u32(data, 4+i*4)
	}
	dataStart := 4 + numEntries*4

	var result interface{}
	if isObj {
		result = parseJSONBObject(data, entries, dataStart, count)
	} else {
		result = parseJSONBArray(data, entries, dataStart, count)
	}

	if header&jbFScalar != 0 {
		if arr, ok := result.([]interface{}); ok && len(arr) == 1 {
			return arr[0]
		}
	}
	return result
}

func parseJSONBObject(data []byte, entries []uint32, dataStart, count int) map[string]interface{} {
	keys, vals := entries[:count], entries[count:]
	keysLen := totalLen(keys)

	result := make(map[string]interface{}, count)
	for i := 0; i < count; i++ {
		kOff, kLen := entryOffLen(keys, i, 0)
		key := ""
		if dataStart+kOff+kLen <= len(data) {
			key = string(data[dataStart+kOff : dataStart+kOff+kLen])
		}

		vOff, vLen := entryOffLen(vals, i, keysLen)
		result[key] = decodeJEntry(data, dataStart+vOff, vLen, vals[i])
	}
	return result
}

func parseJSONBArray(data []byte, entries []uint32, dataStart, count int) []interface{} {
	result := make([]interface{}, count)
	for i := 0; i < count; i++ {
		off, length := entryOffLen(entries, i, 0)
		result[i] = decodeJEntry(data, dataStart+off, length, entries[i])
	}
	return result
}

func totalLen(entries []uint32) int {
	if len(entries) == 0 {
		return 0
	}
	return endOffset(entries, len(entries)-1)
}

func entryOffLen(entries []uint32, idx, base int) (int, int) {
	je := entries[idx]
	val := int(je & jeOffMask)
	start := 0
	if idx > 0 {
		start = endOffset(entries, idx-1)
	}
	if je&jeHasOff != 0 {
		return base + start, val - start
	}
	return base + start, val
}

func endOffset(entries []uint32, idx int) int {
	for i := idx; i >= 0; i-- {
		if entries[i]&jeHasOff != 0 {
			base := int(entries[i] & jeOffMask)
			for j := i + 1; j <= idx; j++ {
				base += int(entries[j] & jeOffMask)
			}
			return base
		}
	}
	sum := 0
	for i := 0; i <= idx; i++ {
		sum += int(entries[i] & jeOffMask)
	}
	return sum
}

func decodeJEntry(data []byte, off, length int, je uint32) interface{} {
	switch je & 0x70000000 {
	case jeString:
		if off+length <= len(data) {
			return string(data[off : off+length])
		}
	case jeNumeric:
		aligned := align(off, 4)
		pad := aligned - off
		if pad < length && aligned+length-pad <= len(data) {
			return decodeJNumeric(data[aligned : aligned+length-pad])
		}
	case jeContainer:
		aligned := align(off, 4)
		pad := aligned - off
		if pad < length && aligned+length-pad <= len(data) {
			return ParseJSONB(data[aligned : aligned+length-pad])
		}
	case jeNull:
		return nil
	case jeBoolFalse:
		return false
	case jeBoolTrue:
		return true
	}
	return nil
}

func decodeJNumeric(data []byte) interface{} {
	if len(data) < 4 {
		return nil
	}
	hdr := u32(data, 0)
	var content []byte
	if hdr&3 == 0 {
		n := int(hdr >> 2)
		if n > 4 && len(data) >= n {
			content = data[4:n]
		}
	} else {
		n := int((hdr & 0xFF) >> 1)
		if n > 1 && len(data) >= n {
			content = data[1:n]
		}
	}
	return DecodeNumeric(content)
}

// DecodeNumeric decodes PostgreSQL numeric type
func DecodeNumeric(raw []byte) interface{} {
	if len(raw) < 2 {
		return nil
	}

	header := u16(raw, 0)
	if header&0x8000 != 0 {
		return decodeNumericShort(raw, header)
	}
	return decodeNumericLong(raw)
}

func decodeNumericShort(raw []byte, header uint16) interface{} {
	sign := 1
	if header&0x2000 != 0 {
		sign = -1
	}
	weight := int(header & 0x003F)
	if header&0x0040 != 0 {
		weight = -weight - 1
	}

	ndigits := (len(raw) - 2) / 2
	if ndigits == 0 {
		return 0
	}

	digits := make([]int, ndigits)
	for i := 0; i < ndigits; i++ {
		digits[i] = int(u16(raw, 2+i*2))
	}
	return computeNumeric(digits, weight, sign)
}

func decodeNumericLong(raw []byte) interface{} {
	if len(raw) < 8 {
		return nil
	}
	ndigits := int(u16(raw, 0))
	weight := int(i16(raw, 2))
	sign := 1
	if u16(raw, 4) == 0x4000 {
		sign = -1
	}
	if ndigits == 0 {
		return 0
	}
	if len(raw) < 8+ndigits*2 {
		return nil
	}

	digits := make([]int, ndigits)
	for i := 0; i < ndigits; i++ {
		digits[i] = int(u16(raw, 8+i*2))
	}
	return computeNumeric(digits, weight, sign)
}

func computeNumeric(digits []int, weight, sign int) float64 {
	if len(digits) == 0 {
		return 0
	}
	result := float64(0)
	for _, d := range digits {
		result = result*10000 + float64(d)
	}
	exp := weight - len(digits) + 1
	if exp >= 0 {
		result *= math.Pow(10000, float64(exp))
	} else {
		result /= math.Pow(10000, float64(-exp))
	}
	return float64(sign) * result
}
