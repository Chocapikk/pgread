package pkg

// ReadTuples extracts all visible tuples from heap file data
func ReadTuples(data []byte, visibleOnly bool) []TupleEntry {
	if len(data) == 0 {
		return nil
	}

	var entries []TupleEntry

	for offset := 0; offset+PageSize <= len(data); offset += PageSize {
		page := data[offset : offset+PageSize]
		if !IsValidPage(page) {
			continue
		}

		pageEntries := ExtractTuples(page)
		for _, entry := range pageEntries {
			if visibleOnly && !IsVisible(entry.Tuple.Header) {
				continue
			}
			entry.PageOffset = offset
			entries = append(entries, entry)
		}
	}

	return entries
}

// ReadRows decodes tuples using column schema
func ReadRows(data []byte, columns []Column, visibleOnly bool) []map[string]interface{} {
	tuples := ReadTuples(data, visibleOnly)
	var rows []map[string]interface{}

	for _, t := range tuples {
		row := DecodeTuple(t.Tuple, columns)
		if row != nil {
			rows = append(rows, row)
		}
	}

	return rows
}

// DecodeTuple decodes a tuple using column schema
func DecodeTuple(tuple *HeapTupleData, columns []Column) map[string]interface{} {
	if tuple == nil || tuple.Data == nil {
		return nil
	}

	result := make(map[string]interface{})
	offset := 0

	for idx, col := range columns {
		name := col.Name
		if name == "" {
			name = "col" + string(rune('1'+idx))
		}

		offset = alignOffset(offset, col.TypID, col.Len)

		num := col.Num
		if num == 0 {
			num = idx + 1
		}

		if IsNull(tuple.Bitmap, num) {
			result[name] = nil
			continue
		}

		val, consumed := readValue(tuple.Data, offset, col.TypID, col.Len)
		result[name] = val
		offset += consumed
	}

	return result
}

func alignOffset(offset, typID, length int) int {
	align := typeAlignment(typID, length)
	if align <= 1 {
		return offset
	}
	return (offset + align - 1) &^ (align - 1)
}

func typeAlignment(typID, length int) int {
	// Variable length types
	if length == -1 {
		return 1
	}

	// 8-byte aligned types
	switch typID {
	case OidInt8, OidFloat8, OidTimestamp, OidTimestampTZ:
		return 8
	}

	// 4-byte aligned types
	switch typID {
	case OidInt4, OidOid, OidFloat4:
		return 4
	}
	if length == 4 {
		return 4
	}

	// 2-byte aligned types
	if typID == OidInt2 || length == 2 {
		return 2
	}

	return 1
}

func readValue(data []byte, offset, typID, length int) (interface{}, int) {
	if offset >= len(data) {
		return nil, 0
	}

	remaining := data[offset:]

	// Fixed-length type
	if length > 0 {
		if len(remaining) < length {
			return nil, 0
		}
		return DecodeType(remaining[:length], typID), length
	}

	// Variable-length type (varlena)
	if length == -1 {
		val, consumed := ReadVarlena(remaining)
		if val == nil {
			return nil, max(consumed, 1)
		}
		return DecodeType(val, typID), consumed
	}

	// C-string (null-terminated)
	idx := 0
	for idx < len(remaining) && remaining[idx] != 0 {
		idx++
	}
	return string(remaining[:idx]), idx + 1
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
