package pkg

import "encoding/binary"

const (
	TupleHeaderSize = 23
	NattsMask       = 0x07FF
)

// Tuple infomask flags
const (
	HasNull        = 0x0001
	HasVarwidth    = 0x0002
	HasExternal    = 0x0004
	XminCommitted  = 0x0100
	XminInvalid    = 0x0200
	XmaxCommitted  = 0x0400
	XmaxInvalid    = 0x0800
)

// Tuple infomask2 flags
const (
	HotUpdated = 0x4000
	HeapOnly   = 0x8000
)

// HeapTupleHeader represents tuple header
type HeapTupleHeader struct {
	TXmin        uint32
	TXmax        uint32
	TCid         uint32
	TCtidBlock   uint32
	TCtidOffset  uint16
	TInfomask    uint16
	TInfomask2   uint16
	THoff        uint8
	Natts        int
	HasNull      bool
	HasVarwidth  bool
	HasExternal  bool
	XminCommitted bool
	XminInvalid  bool
	XmaxCommitted bool
	XmaxInvalid  bool
	HotUpdated   bool
	HeapOnly     bool
}

// HeapTupleData represents a complete tuple
type HeapTupleData struct {
	Header *HeapTupleHeader
	Bitmap []byte
	Data   []byte
}

// ParseHeapTuple parses a heap tuple from raw data
func ParseHeapTuple(data []byte) *HeapTupleData {
	if len(data) < TupleHeaderSize {
		return nil
	}

	header := parseHeader(data)
	if int(header.THoff) > len(data) {
		return nil
	}

	tuple := &HeapTupleData{
		Header: header,
		Data:   data[header.THoff:],
	}

	if header.HasNull {
		tuple.Bitmap = extractBitmap(data, header)
	}

	return tuple
}

func parseHeader(data []byte) *HeapTupleHeader {
	infomask := binary.LittleEndian.Uint16(data[20:22])
	infomask2 := binary.LittleEndian.Uint16(data[18:20])

	return &HeapTupleHeader{
		TXmin:        binary.LittleEndian.Uint32(data[0:4]),
		TXmax:        binary.LittleEndian.Uint32(data[4:8]),
		TCid:         binary.LittleEndian.Uint32(data[8:12]),
		TCtidBlock:   binary.LittleEndian.Uint32(data[12:16]),
		TCtidOffset:  binary.LittleEndian.Uint16(data[16:18]),
		TInfomask:    infomask,
		TInfomask2:   infomask2,
		THoff:        data[22],
		Natts:        int(infomask2 & NattsMask),
		HasNull:      (infomask & HasNull) != 0,
		HasVarwidth:  (infomask & HasVarwidth) != 0,
		HasExternal:  (infomask & HasExternal) != 0,
		XminCommitted: (infomask & XminCommitted) != 0,
		XminInvalid:  (infomask & XminInvalid) != 0,
		XmaxCommitted: (infomask & XmaxCommitted) != 0,
		XmaxInvalid:  (infomask & XmaxInvalid) != 0,
		HotUpdated:   (infomask2 & HotUpdated) != 0,
		HeapOnly:     (infomask2 & HeapOnly) != 0,
	}
}

func extractBitmap(data []byte, header *HeapTupleHeader) []byte {
	if !header.HasNull {
		return nil
	}

	bytes := (header.Natts + 7) / 8
	if len(data) < TupleHeaderSize+bytes {
		return nil
	}

	return data[TupleHeaderSize : TupleHeaderSize+bytes]
}

// IsVisible checks if tuple is visible (committed and not deleted)
func IsVisible(header *HeapTupleHeader) bool {
	return header != nil && header.XminCommitted && (header.XmaxInvalid || !header.XmaxCommitted)
}

// IsNull checks if attribute at attnum is null
func IsNull(bitmap []byte, attnum int) bool {
	if bitmap == nil || attnum <= 0 {
		return false
	}

	byteIdx := (attnum - 1) / 8
	bitIdx := (attnum - 1) % 8

	if byteIdx >= len(bitmap) {
		return true
	}

	return (bitmap[byteIdx] & (1 << bitIdx)) == 0
}
