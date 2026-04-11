package pgdump

const tupleHeaderSize = 23

// HeapTupleHeader contains tuple metadata
type HeapTupleHeader struct {
	THoff         uint8
	Natts         int
	Infomask      uint16
	Xmin          uint32
	Xmax          uint32
	XminCommitted bool
	XmaxInvalid   bool
	XmaxCommitted bool
	HasNull       bool
}

// HeapTupleData represents a complete tuple
type HeapTupleData struct {
	Header *HeapTupleHeader
	Bitmap []byte
	Data   []byte
}

// ParseHeapTuple parses a heap tuple from raw data
func ParseHeapTuple(data []byte) *HeapTupleData {
	if len(data) < tupleHeaderSize {
		return nil
	}

	xmin := u32(data, 0)
	xmax := u32(data, 4)
	infomask := u16(data, 20)
	infomask2 := u16(data, 18)
	hoff := data[22]

	if int(hoff) > len(data) {
		return nil
	}

	header := &HeapTupleHeader{
		THoff:         hoff,
		Natts:         int(infomask2 & 0x07FF),
		Infomask:      infomask,
		Xmin:          xmin,
		Xmax:          xmax,
		HasNull:       infomask&0x0001 != 0,
		XminCommitted: infomask&0x0100 != 0,
		XmaxCommitted: infomask&0x0400 != 0,
		XmaxInvalid:   infomask&0x0800 != 0,
	}

	tuple := &HeapTupleData{
		Header: header,
		Data:   data[hoff:],
	}

	if header.HasNull {
		bitmapBytes := (header.Natts + 7) / 8
		if len(data) >= tupleHeaderSize+bitmapBytes {
			tuple.Bitmap = data[tupleHeaderSize : tupleHeaderSize+bitmapBytes]
		}
	}

	return tuple
}

// IsVisible checks if tuple is visible.
// Without access to PostgreSQL's CLOG, we cannot confirm xmin committed status
// (hint bits may not be set for recently inserted or VACUUM FULL'd tuples).
// Instead, accept all tuples that are not provably dead.
func (t *HeapTupleData) IsVisible() bool {
	h := t.Header
	// Provably dead: xmax is committed and not invalid
	if h.XmaxCommitted && !h.XmaxInvalid {
		return false
	}
	// Provably dead: xmax set (targeted by DELETE/UPDATE) and no hint bits yet
	if h.Xmax != 0 && !h.XmaxInvalid {
		return false
	}
	return true
}

// IsNull checks if attribute at position is null (1-indexed)
func (t *HeapTupleData) IsNull(attnum int) bool {
	if t.Bitmap == nil || attnum <= 0 {
		return false
	}
	byteIdx, bitIdx := (attnum-1)/8, (attnum-1)%8
	if byteIdx >= len(t.Bitmap) {
		return true
	}
	return t.Bitmap[byteIdx]&(1<<bitIdx) == 0
}
