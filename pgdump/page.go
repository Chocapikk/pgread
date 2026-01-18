package pgdump

const (
	PageSize   = 8192
	headerSize = 24
	itemIDSize = 4
)

// PageHeader represents PostgreSQL page header
type PageHeader struct {
	Lower, Upper uint16
	PageSize     int
	Version      int
}

// ItemID represents a line pointer
type ItemID struct {
	Offset, Length, Flags int
}

// TupleEntry combines ItemID with parsed tuple
type TupleEntry struct {
	Tuple      *HeapTupleData
	PageOffset int
}

// ParsePage extracts all visible tuples from a page
func ParsePage(data []byte) []TupleEntry {
	if len(data) < PageSize {
		return nil
	}

	h := parseHeader(data)
	if !validHeader(h) {
		return nil
	}

	var entries []TupleEntry
	for _, item := range parseItems(data, h) {
		if item.Flags != 1 || item.Length <= 0 {
			continue
		}
		if item.Offset < int(h.Upper) || item.Offset+item.Length > PageSize {
			continue
		}

		tuple := ParseHeapTuple(data[item.Offset : item.Offset+item.Length])
		if tuple != nil {
			entries = append(entries, TupleEntry{Tuple: tuple})
		}
	}
	return entries
}

func parseHeader(data []byte) *PageHeader {
	psv := u16(data, 18)
	return &PageHeader{
		Lower:    u16(data, 12),
		Upper:    u16(data, 14),
		PageSize: int(psv & 0xFF00),
		Version:  int(psv & 0x00FF),
	}
}

func parseItems(data []byte, h *PageHeader) []ItemID {
	var items []ItemID
	for off := headerSize; off < int(h.Lower); off += itemIDSize {
		raw := u32(data, off)
		items = append(items, ItemID{
			Offset: int(raw & 0x7FFF),
			Flags:  int((raw >> 15) & 0x03),
			Length: int((raw >> 17) & 0x7FFF),
		})
	}
	return items
}

func validHeader(h *PageHeader) bool {
	validSizes := map[int]bool{8192: true, 16384: true, 32768: true}
	return h != nil &&
		validSizes[h.PageSize] &&
		h.Version >= 1 && h.Version <= 10 &&
		int(h.Lower) >= headerSize &&
		int(h.Upper) <= h.PageSize &&
		h.Lower <= h.Upper
}
