package pkg

import (
	"encoding/binary"
	"math"
)

// JSONB header flags
const (
	JBCMask   = 0x0FFFFFFF
	JBFScalar = 0x10000000
	JBFObject = 0x20000000
	JBFArray  = 0x40000000
)

// JEntry flags
const (
	JEOffLenMask  = 0x0FFFFFFF
	JEHasOff      = 0x80000000
	JEIsString    = 0x00000000
	JEIsNumeric   = 0x10000000
	JEIsBoolFalse = 0x20000000
	JEIsBoolTrue  = 0x30000000
	JEIsNull      = 0x40000000
	JEIsContainer = 0x50000000
)

// ParseJSONB parses PostgreSQL JSONB binary format
func ParseJSONB(data []byte) interface{} {
	if len(data) < 4 {
		return nil
	}

	header := binary.LittleEndian.Uint32(data[0:4])
	count := int(header & JBCMask)
	isObj := (header & JBFObject) != 0
	isArr := (header & JBFArray) != 0
	isScalar := (header & JBFScalar) != 0

	if (!isObj && !isArr) || count <= 0 || count > 10000 {
		return nil
	}

	numEntries := count
	if isObj {
		numEntries = count * 2
	}

	if 4+numEntries*4 > len(data) {
		return nil
	}

	entries := make([]uint32, numEntries)
	for i := 0; i < numEntries; i++ {
		entries[i] = binary.LittleEndian.Uint32(data[4+i*4 : 8+i*4])
	}

	dataStart := 4 + numEntries*4

	var result interface{}
	if isObj {
		result = parseJSONBObject(data, entries, dataStart, count)
	} else {
		result = parseJSONBArray(data, entries, dataStart, count)
	}

	if isScalar {
		if arr, ok := result.([]interface{}); ok && len(arr) == 1 {
			return arr[0]
		}
	}

	return result
}

func parseJSONBObject(data []byte, entries []uint32, dataStart, count int) map[string]interface{} {
	keyEntries := entries[:count]
	valEntries := entries[count:]

	keysTotalLen := calcTotalLength(keyEntries)

	result := make(map[string]interface{})
	for i := 0; i < count; i++ {
		keyOff, keyLen := calcEntryOffsetLen(keyEntries, i, 0)
		key := ""
		if dataStart+keyOff+keyLen <= len(data) {
			key = string(data[dataStart+keyOff : dataStart+keyOff+keyLen])
		}

		valOff, valLen := calcEntryOffsetLen(valEntries, i, keysTotalLen)
		val := decodeJSONBValue(data, dataStart+valOff, valLen, valEntries[i])

		result[key] = val
	}
	return result
}

func parseJSONBArray(data []byte, entries []uint32, dataStart, count int) []interface{} {
	result := make([]interface{}, count)
	for i := 0; i < count; i++ {
		off, length := calcEntryOffsetLen(entries, i, 0)
		result[i] = decodeJSONBValue(data, dataStart+off, length, entries[i])
	}
	return result
}

func calcTotalLength(entries []uint32) int {
	if len(entries) == 0 {
		return 0
	}
	return findPrevEndOffset(entries, len(entries)-1)
}

func calcEntryOffsetLen(entries []uint32, idx, baseOffset int) (int, int) {
	je := entries[idx]
	val := int(je & JEOffLenMask)

	if (je & JEHasOff) != 0 {
		// val is end offset
		startOff := 0
		if idx > 0 {
			startOff = findPrevEndOffset(entries, idx-1)
		}
		return baseOffset + startOff, val - startOff
	}

	// val is length
	startOff := 0
	if idx > 0 {
		startOff = findPrevEndOffset(entries, idx-1)
	}
	return baseOffset + startOff, val
}

func findPrevEndOffset(entries []uint32, idx int) int {
	// Walk backwards to find entry with HAS_OFF
	for i := idx; i >= 0; i-- {
		je := entries[i]
		if (je & JEHasOff) != 0 {
			base := int(je & JEOffLenMask)
			extra := 0
			for j := i + 1; j <= idx; j++ {
				extra += int(entries[j] & JEOffLenMask)
			}
			return base + extra
		}
	}
	// No HAS_OFF found, sum all lengths
	total := 0
	for i := 0; i <= idx; i++ {
		total += int(entries[i] & JEOffLenMask)
	}
	return total
}

func decodeJSONBValue(data []byte, offset, length int, je uint32) interface{} {
	typ := je & 0x70000000

	switch typ {
	case JEIsString:
		if offset+length > len(data) {
			return nil
		}
		return string(data[offset : offset+length])

	case JEIsNumeric:
		return decodeJSONBNumeric(data, offset, length)

	case JEIsContainer:
		// Containers are INTALIGN'd
		alignedOff := (offset + 3) &^ 3
		padding := alignedOff - offset
		actualLen := length - padding
		if alignedOff+actualLen > len(data) {
			return nil
		}
		return ParseJSONB(data[alignedOff : alignedOff+actualLen])

	case JEIsNull:
		return nil

	case JEIsBoolFalse:
		return false

	case JEIsBoolTrue:
		return true

	default:
		if offset+length > len(data) {
			return nil
		}
		return string(data[offset : offset+length])
	}
}

func decodeJSONBNumeric(data []byte, offset, length int) interface{} {
	alignedOff := (offset + 3) &^ 3
	padding := alignedOff - offset
	if padding >= length {
		return nil
	}

	varlenaData := data[alignedOff : alignedOff+length-padding]
	if len(varlenaData) < 4 {
		return nil
	}

	vlHeader := binary.LittleEndian.Uint32(varlenaData[0:4])
	var numericContent []byte

	if (vlHeader & 3) == 0 {
		// Long varlena
		vlLen := int(vlHeader >> 2)
		if vlLen > 4 && len(varlenaData) >= vlLen {
			numericContent = varlenaData[4:vlLen]
		}
	} else {
		// Short varlena
		vlLen := int((vlHeader & 0xFF) >> 1)
		if vlLen > 1 && len(varlenaData) >= vlLen {
			numericContent = varlenaData[1:vlLen]
		}
	}

	return DecodeNumeric(numericContent)
}

// Numeric decoding constants
const (
	NumericNBase               = 10000
	NumericShort               = 0x8000
	NumericShortSignMask       = 0x2000
	NumericShortWeightSignMask = 0x0040
	NumericShortWeightMask     = 0x003F
)

// DecodeNumeric decodes PostgreSQL numeric type
func DecodeNumeric(raw []byte) interface{} {
	if len(raw) < 2 {
		return nil
	}

	header := binary.LittleEndian.Uint16(raw[0:2])

	if (header & NumericShort) != 0 {
		return decodeNumericShort(raw, header)
	}
	return decodeNumericLong(raw)
}

func decodeNumericShort(raw []byte, header uint16) interface{} {
	sign := 1
	if (header & NumericShortSignMask) != 0 {
		sign = -1
	}

	weight := int(header & NumericShortWeightMask)
	if (header & NumericShortWeightSignMask) != 0 {
		weight = -weight - 1
	}

	ndigits := (len(raw) - 2) / 2
	if ndigits == 0 {
		return 0
	}

	digits := make([]int, ndigits)
	for i := 0; i < ndigits; i++ {
		digits[i] = int(binary.LittleEndian.Uint16(raw[2+i*2 : 4+i*2]))
	}

	return computeNumericValue(digits, weight, sign)
}

func decodeNumericLong(raw []byte) interface{} {
	if len(raw) < 8 {
		return nil
	}

	ndigits := int(binary.LittleEndian.Uint16(raw[0:2]))
	weight := int(int16(binary.LittleEndian.Uint16(raw[2:4])))
	signRaw := binary.LittleEndian.Uint16(raw[4:6])
	sign := 1
	if signRaw == 0x4000 {
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
		digits[i] = int(binary.LittleEndian.Uint16(raw[8+i*2 : 10+i*2]))
	}

	return computeNumericValue(digits, weight, sign)
}

func computeNumericValue(digits []int, weight, sign int) interface{} {
	if len(digits) == 0 {
		return 0
	}

	result := float64(0)
	for _, d := range digits {
		result = result*NumericNBase + float64(d)
	}

	exp := weight - len(digits) + 1

	if exp >= 0 {
		result *= math.Pow(NumericNBase, float64(exp))
	} else {
		result /= math.Pow(NumericNBase, float64(-exp))
	}

	return float64(sign) * result
}
