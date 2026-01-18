package pgdump

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
	var rows []map[string]interface{}
	for _, t := range ReadTuples(data, visibleOnly) {
		if row := DecodeTuple(t.Tuple, columns); row != nil {
			rows = append(rows, row)
		}
	}
	return rows
}

// DecodeTuple decodes a tuple using column schema
func DecodeTuple(tuple *HeapTupleData, columns []Column) map[string]interface{} {
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

		offset = align(offset, typeAlign(col.TypID, col.Len))

		if tuple.IsNull(num) {
			result[col.Name] = nil
			continue
		}

		val, consumed := readValue(tuple.Data, offset, col.TypID, col.Len)
		result[col.Name] = val
		offset += consumed
	}

	return result
}

func typeAlign(typID, length int) int {
	if length == -1 {
		return 1
	}
	switch typID {
	case OidInt8, OidFloat8, OidTimestamp, OidTimestampTZ:
		return 8
	case OidInt4, OidOid, OidFloat4:
		return 4
	case OidInt2:
		return 2
	}
	if length == 4 {
		return 4
	}
	if length == 2 {
		return 2
	}
	return 1
}

func readValue(data []byte, offset, typID, length int) (interface{}, int) {
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
		val, consumed := ReadVarlena(remaining)
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

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
