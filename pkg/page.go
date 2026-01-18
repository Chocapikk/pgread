// Package pkg provides PostgreSQL heap file parsing functionality
package pkg

import (
	"encoding/binary"
)

const (
	PageSize   = 8192
	HeaderSize = 24
	ItemIDSize = 4
)

var ValidPageSizes = []int{8192, 16384, 32768}

// PageHeader represents PostgreSQL page header (bufpage.h)
type PageHeader struct {
	PDLsn            uint64
	PDChecksum       uint16
	PDFlags          uint16
	PDLower          uint16
	PDUpper          uint16
	PDSpecial        uint16
	PDPagesizeVersion uint16
	PDPruneXid       uint32
	PageSize         int
	Version          int
}

// ItemID represents a line pointer
type ItemID struct {
	Num    int
	Offset int
	Flags  int
	Length int
}

// TupleEntry combines ItemID with parsed tuple
type TupleEntry struct {
	Item       ItemID
	Tuple      *HeapTupleData
	PageOffset int
}

// ParsePageHeader parses page header from raw data
func ParsePageHeader(data []byte) *PageHeader {
	if len(data) < HeaderSize {
		return nil
	}

	h := &PageHeader{
		PDLsn:            binary.LittleEndian.Uint64(data[0:8]),
		PDChecksum:       binary.LittleEndian.Uint16(data[8:10]),
		PDFlags:          binary.LittleEndian.Uint16(data[10:12]),
		PDLower:          binary.LittleEndian.Uint16(data[12:14]),
		PDUpper:          binary.LittleEndian.Uint16(data[14:16]),
		PDSpecial:        binary.LittleEndian.Uint16(data[16:18]),
		PDPagesizeVersion: binary.LittleEndian.Uint16(data[18:20]),
		PDPruneXid:       binary.LittleEndian.Uint32(data[20:24]),
	}
	h.PageSize = int(h.PDPagesizeVersion & 0xFF00)
	h.Version = int(h.PDPagesizeVersion & 0x00FF)

	return h
}

// ParseItemIDs extracts line pointers from page
func ParseItemIDs(data []byte, header *PageHeader) []ItemID {
	if header == nil || int(header.PDLower) <= HeaderSize {
		return nil
	}

	var items []ItemID
	num := 1
	for offset := HeaderSize; offset < int(header.PDLower); offset += ItemIDSize {
		if offset+ItemIDSize > len(data) {
			break
		}
		raw := binary.LittleEndian.Uint32(data[offset : offset+ItemIDSize])
		items = append(items, ItemID{
			Num:    num,
			Offset: int(raw & 0x7FFF),
			Flags:  int((raw >> 15) & 0x03),
			Length: int((raw >> 17) & 0x7FFF),
		})
		num++
	}
	return items
}

// ExtractTuples extracts all tuples from a page
func ExtractTuples(data []byte) []TupleEntry {
	if !IsValidPage(data) {
		return nil
	}

	header := ParsePageHeader(data)
	items := ParseItemIDs(data, header)
	
	var entries []TupleEntry
	for _, item := range items {
		entry := extractTuple(data, header, item)
		if entry != nil {
			entries = append(entries, *entry)
		}
	}
	return entries
}

func extractTuple(data []byte, header *PageHeader, item ItemID) *TupleEntry {
	// LP_NORMAL = 1
	if item.Flags != 1 || item.Length <= 0 {
		return nil
	}
	if item.Offset < int(header.PDUpper) || item.Offset+item.Length > PageSize {
		return nil
	}

	tupleData := data[item.Offset : item.Offset+item.Length]
	tuple := ParseHeapTuple(tupleData)
	if tuple == nil {
		return nil
	}

	return &TupleEntry{
		Item:  item,
		Tuple: tuple,
	}
}

// IsValidPage checks if data is a valid PostgreSQL page
func IsValidPage(data []byte) bool {
	if len(data) < PageSize {
		return false
	}

	h := ParsePageHeader(data)
	return isValidHeader(h)
}

func isValidHeader(h *PageHeader) bool {
	if h == nil {
		return false
	}

	validSize := false
	for _, s := range ValidPageSizes {
		if h.PageSize == s {
			validSize = true
			break
		}
	}

	return validSize &&
		h.Version >= 1 && h.Version <= 10 &&
		int(h.PDLower) >= HeaderSize &&
		int(h.PDUpper) <= h.PageSize &&
		int(h.PDLower) <= int(h.PDUpper)
}
