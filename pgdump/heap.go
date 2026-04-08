package pgdump

import "fmt"

// ReadTuples extracts all visible tuples from heap file data
func ReadTuples(data []byte, visibleOnly bool) []TupleEntry {
	var entries []TupleEntry
	for off := 0; off+PageSize <= len(data); off += PageSize {
		for _, e := range ParsePage(data[off : off+PageSize]) {
			if !visibleOnly || e.Tuple.IsVisible() {
				e.PageOffset = off
				entries = append(entries, e)
			}
		}
	}
	return entries
}

// ReadRows decodes tuples using column schema
func ReadRows(data []byte, columns []Column, visibleOnly bool) []map[string]interface{} {
	return ReadRowsWithTOAST(data, columns, visibleOnly, nil)
}

// ReadRowsWithTOAST decodes tuples using column schema, resolving TOAST pointers.
func ReadRowsWithTOAST(data []byte, columns []Column, visibleOnly bool, toastReader *TOASTReader) []map[string]interface{} {
	var rows []map[string]interface{}
	for _, t := range ReadTuples(data, visibleOnly) {
		if row := DecodeTupleWithTOAST(t.Tuple, columns, toastReader); row != nil {
			rows = append(rows, row)
		}
	}
	return rows
}

// Debug enables debug output for tuple decoding
var Debug bool
// DebugTable filters debug output to specific table name
var DebugTable string

// DecodeTuple decodes a tuple using column schema
func DecodeTuple(tuple *HeapTupleData, columns []Column) map[string]interface{} {
	return DecodeTupleWithTOAST(tuple, columns, nil)
}

// DecodeTupleWithTOAST decodes a tuple, resolving TOAST pointers via the reader.
func DecodeTupleWithTOAST(tuple *HeapTupleData, columns []Column, toastReader *TOASTReader) map[string]interface{} {
	if tuple == nil || len(tuple.Data) == 0 {
		return nil
	}

	result := make(map[string]interface{}, len(columns))
	offset := 0

	for idx, col := range columns {
		num := col.Num
		if num == 0 {
			num = idx + 1
		}

		// Determine column alignment
		colAlign := alignFromChar(col.Align)
		if colAlign == 0 {
			colAlign = typeAlign(col.TypID, col.Len)
		}

		// PostgreSQL att_align_pointer / VARATT_NOT_PAD_BYTE:
		// If the byte at the current offset is non-zero, the varlena data
		// starts here (short varlena, TOAST pointer, or already-aligned long
		// varlena). Zero bytes are padding, so we align normally.
		if col.Len == -1 && offset < len(tuple.Data) && tuple.Data[offset] != 0 {
			colAlign = 1
		}

		prevOffset := offset
		offset = align(offset, colAlign)

		if tuple.IsNull(num) {
			if Debug {
				bitmapInfo := "no bitmap"
				if tuple.Bitmap != nil {
					byteIdx := (num - 1) / 8
					bitIdx := (num - 1) % 8
					if byteIdx < len(tuple.Bitmap) {
						bitmapInfo = fmt.Sprintf("bitmap[%d]=%08b bit%d=0", byteIdx, tuple.Bitmap[byteIdx], bitIdx)
					} else {
						bitmapInfo = fmt.Sprintf("bitmap len=%d < needed %d", len(tuple.Bitmap), byteIdx+1)
					}
				}
				fmt.Printf("DEBUG: col=%s num=%d offset=%d (align=%d) NULL (%s)\n", col.Name, num, offset, colAlign, bitmapInfo)
			}
			result[col.Name] = nil
			continue
		}

		val, consumed := readValueWithTOAST(tuple.Data, offset, col.TypID, col.Len, toastReader)
		if Debug {
			dataPreview := ""
			if offset < len(tuple.Data) {
				end := offset + 20
				if end > len(tuple.Data) {
					end = len(tuple.Data)
				}
				dataPreview = fmt.Sprintf(" raw=%x", tuple.Data[offset:end])
			}
			fmt.Printf("DEBUG: col=%s num=%d offset=%d->%d (align=%d/%c) len=%d consumed=%d val=%v%s\n",
				col.Name, num, prevOffset, offset, colAlign, col.Align, col.Len, consumed, val, dataPreview)
		}
		result[col.Name] = val
		offset += consumed
	}

	return result
}

// alignFromChar converts PostgreSQL alignment char to bytes
func alignFromChar(c byte) int {
	switch c {
	case 'c':
		return 1
	case 's':
		return 2
	case 'i':
		return 4
	case 'd':
		return 8
	default:
		return 0 // unknown, use typeAlign fallback
	}
}

func typeAlign(typID, length int) int {
	// PostgreSQL type alignments from pg_type.dat
	// 'd' = 8 (double), 'i' = 4 (int), 's' = 2 (short), 'c' = 1 (char)
	
	switch typID {
	// Double alignment (8 bytes)
	case OidInt8, OidFloat8, OidTimestamp, OidTimestampTZ, OidTime, OidMoney, OidPgLsn:
		return 8
	case OidPoint, OidLseg, OidBox, OidLine, OidCircle: // geometric types
		return 8
	case OidInterval, OidTimeTZ: // 16 and 12 bytes but 'd' aligned
		return 8
		
	// Int alignment (4 bytes)
	case OidInt4, OidOid, OidFloat4, OidDate, OidXid, OidCid:
		return 4
	// Varlena types are 'i' aligned (4 bytes) for the header
	case OidText, OidVarchar, OidBpchar, OidBytea, OidJSON, OidJSONB, OidXML:
		return 4
	case OidNumeric, OidInet, OidCidr, OidPath, OidPolygon:
		return 4
	case OidBit, OidVarbit, OidTsvector, OidTsquery, OidJSONPath:
		return 4
	case OidInt4Range, OidInt8Range, OidNumRange, OidDateRange, OidTsRange, OidTsTzRange:
		return 4
		
	// Short alignment (2 bytes)
	case OidInt2, OidTid:
		return 2
		
	// Int alignment (4 bytes) - macaddr types
	case OidMacaddr, OidMacaddr8:
		return 4
		
	// Char alignment (1 byte)
	case OidBool, OidChar, OidName, OidUUID:
		return 1
	}
	
	// Default based on length
	if length == -1 {
		return 4 // varlena default
	}
	if length >= 8 {
		return 8
	}
	if length >= 4 {
		return 4
	}
	if length >= 2 {
		return 2
	}
	return 1
}

func readValue(data []byte, offset, typID, length int) (interface{}, int) {
	return readValueWithTOAST(data, offset, typID, length, nil)
}

func readValueWithTOAST(data []byte, offset, typID, length int, toastReader *TOASTReader) (interface{}, int) {
	if offset >= len(data) {
		return nil, 0
	}
	remaining := data[offset:]

	if length > 0 {
		if len(remaining) < length {
			return nil, 0
		}
		return DecodeType(remaining[:length], typID), length
	}

	if length == -1 {
		var val []byte
		var consumed int
		if toastReader != nil {
			val, consumed = ReadVarlenaWithTOAST(remaining, toastReader)
		} else {
			val, consumed = ReadVarlena(remaining)
		}
		if val == nil {
			return nil, max(consumed, 1)
		}
		return DecodeType(val, typID), consumed
	}

	// C-string
	for i, b := range remaining {
		if b == 0 {
			return string(remaining[:i]), i + 1
		}
	}
	return string(remaining), len(remaining)
}

// isShortVarlena checks if data starts with a short varlena header
func isShortVarlena(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	first := data[0]
	// Short varlena: bit 0 is set, but not just 0x01 (which is TOAST)
	return first&1 == 1 && first != 1
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
