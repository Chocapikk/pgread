package pgdump

import (
	"encoding/binary"
	"fmt"
)

// IndexType represents the type of index
type IndexType int

const (
	IndexTypeUnknown IndexType = iota
	IndexTypeBTree
	IndexTypeHash
	IndexTypeGiST
	IndexTypeGIN
	IndexTypeSPGiST
	IndexTypeBRIN
)

func (t IndexType) String() string {
	switch t {
	case IndexTypeBTree:
		return "btree"
	case IndexTypeHash:
		return "hash"
	case IndexTypeGiST:
		return "gist"
	case IndexTypeGIN:
		return "gin"
	case IndexTypeSPGiST:
		return "spgist"
	case IndexTypeBRIN:
		return "brin"
	default:
		return "unknown"
	}
}

// BTree constants
const (
	BTMaxCycleID       = 0xFF00
	BTMetaMagic        = 0x053162
	BTPageMagic        = 0x1234 // Not used, cycle ID range check instead
	
	// BTree page flags
	BTPLeaf           = 1 << 0
	BTPRoot           = 1 << 1
	BTPDeleted        = 1 << 2
	BTPMeta           = 1 << 3
	BTPHalfDead       = 1 << 4
	BTPSplitEnd       = 1 << 5
	BTPHasGarbage     = 1 << 6
	BTPIncompleteSplit = 1 << 7
)

// Hash constants
const (
	HashoPageID = 0xFF80
	
	// Hash page types
	LHUnused   = 0
	LHOverflow = 1 << 0
	LHBucket   = 1 << 1
	LHBitmap   = 1 << 2
	LHMeta     = 1 << 3
)

// GiST constants
const (
	GISTPageID = 0xFF81
	
	// GiST flags
	FLeaf          = 1 << 0
	FDeleted       = 1 << 1
	FTuplesDeleted = 1 << 2
	FFollowRight   = 1 << 3
	FHasGarbage    = 1 << 4
)

// GIN constants
const (
	GINData           = 1 << 0
	GINLeaf           = 1 << 1
	GINDeleted        = 1 << 2
	GINMeta           = 1 << 3
	GINList           = 1 << 4
	GINListFullrow    = 1 << 5
	GINIncompleteSplit = 1 << 6
	GINCompressed     = 1 << 7
)

// SP-GiST constants
const (
	SPGISTPageID = 0xFF82
	
	// SP-GiST flags
	SPGISTMeta    = 1 << 0
	SPGISTDeleted = 1 << 1
	SPGISTLeaf    = 1 << 2
	SPGISTNulls   = 1 << 3
)

// IndexPageInfo contains parsed index page information
type IndexPageInfo struct {
	PageNumber   uint32     `json:"page_number"`
	IndexType    IndexType  `json:"index_type"`
	TypeString   string     `json:"type_string"`
	IsMeta       bool       `json:"is_meta,omitempty"`
	IsLeaf       bool       `json:"is_leaf,omitempty"`
	IsRoot       bool       `json:"is_root,omitempty"`
	IsDeleted    bool       `json:"is_deleted,omitempty"`
	Flags        uint16     `json:"flags"`
	FlagStrings  []string   `json:"flag_strings,omitempty"`
	Level        uint32     `json:"level,omitempty"`
	PrevBlock    uint32     `json:"prev_block,omitempty"`
	NextBlock    uint32     `json:"next_block,omitempty"`
	RightLink    uint32     `json:"right_link,omitempty"`
	ItemCount    int        `json:"item_count"`
	FreeSpace    int        `json:"free_space"`
	LSN          uint64     `json:"lsn"`
	LSNStr       string     `json:"lsn_str"`
}

// BTreeMetaPage contains B-tree metapage information
type BTreeMetaPage struct {
	Magic        uint32 `json:"magic"`
	Version      uint32 `json:"version"`
	Root         uint32 `json:"root"`
	Level        uint32 `json:"level"`
	FastRoot     uint32 `json:"fast_root"`
	FastLevel    uint32 `json:"fast_level"`
	LastCleanupNumHeapTuples float64 `json:"last_cleanup_num_heap_tuples,omitempty"`
	AllequalImage bool   `json:"allequal_image,omitempty"`
}

// HashMetaPage contains hash index metapage information
type HashMetaPage struct {
	Magic           uint32  `json:"magic"`
	Version         uint32  `json:"version"`
	NumBuckets      uint32  `json:"num_buckets"`
	MaxBucket       uint32  `json:"max_bucket"`
	HighMask        uint32  `json:"high_mask"`
	LowMask         uint32  `json:"low_mask"`
	FFactor         uint16  `json:"ffactor"`
	NumKeys         float64 `json:"num_keys"`
	NumTuples       float64 `json:"num_tuples"`
}

// GINMetaPage contains GIN index metapage information
type GINMetaPage struct {
	Version          uint32  `json:"version"`
	Head             uint32  `json:"pending_head"`
	Tail             uint32  `json:"pending_tail"`
	TailFreeSize     uint32  `json:"tail_free_size"`
	NPendingPages    uint32  `json:"n_pending_pages"`
	NPendingHeapTuples uint64 `json:"n_pending_heap_tuples"`
	NTotalPages      uint32  `json:"n_total_pages"`
	NEntryPages      uint32  `json:"n_entry_pages"`
	NDataPages       uint32  `json:"n_data_pages"`
	NEntries         uint64  `json:"n_entries"`
}

// IndexInfo contains full index information
type IndexInfo struct {
	Type      IndexType       `json:"type"`
	TypeString string         `json:"type_string"`
	TotalPages int            `json:"total_pages"`
	Meta       interface{}    `json:"meta,omitempty"`
	Levels     int            `json:"levels,omitempty"`
	RootPage   uint32         `json:"root_page,omitempty"`
	Pages      []IndexPageInfo `json:"pages,omitempty"`
}

// ParseIndexFile parses an index file and returns information about it
func ParseIndexFile(data []byte) (*IndexInfo, error) {
	if len(data) < PageSize {
		return nil, fmt.Errorf("index file too small")
	}
	
	info := &IndexInfo{
		TotalPages: len(data) / PageSize,
	}
	
	// Detect index type from first page's special section
	info.Type = detectIndexType(data[0:PageSize])
	info.TypeString = info.Type.String()
	
	// Parse metapage if present
	switch info.Type {
	case IndexTypeBTree:
		if meta := parseBTreeMeta(data[0:PageSize]); meta != nil {
			info.Meta = meta
			info.RootPage = meta.Root
			info.Levels = int(meta.Level)
		}
	case IndexTypeHash:
		if meta := parseHashMeta(data[0:PageSize]); meta != nil {
			info.Meta = meta
		}
	case IndexTypeGIN:
		if meta := parseGINMeta(data[0:PageSize]); meta != nil {
			info.Meta = meta
		}
	}
	
	// Parse all pages
	for i := 0; i < info.TotalPages; i++ {
		offset := i * PageSize
		page := data[offset : offset+PageSize]
		
		pageInfo := parseIndexPage(page, uint32(i), info.Type)
		info.Pages = append(info.Pages, pageInfo)
	}
	
	return info, nil
}

// detectIndexType attempts to determine the index type from a page
func detectIndexType(page []byte) IndexType {
	if len(page) < PageSize {
		return IndexTypeUnknown
	}
	
	// Get special section offset from page header
	special := binary.LittleEndian.Uint16(page[16:18])
	if special == 0 || int(special) >= PageSize {
		return IndexTypeUnknown
	}
	
	specialSize := PageSize - int(special)
	specialData := page[special:]
	
	// Check for page type identifiers at end of special section
	if specialSize >= 2 {
		pageID := binary.LittleEndian.Uint16(page[PageSize-2:])
		
		switch {
		case pageID == HashoPageID:
			return IndexTypeHash
		case pageID == GISTPageID:
			return IndexTypeGiST
		case pageID == SPGISTPageID:
			return IndexTypeSPGiST
		}
	}
	
	// Check for BTree (uses cycle ID instead of page ID)
	if specialSize >= 16 {
		// BTree opaque data: prev(4) + next(4) + level/flags(4) + cycleID(2)
		cycleID := binary.LittleEndian.Uint16(specialData[14:16])
		flags := binary.LittleEndian.Uint16(specialData[12:14])
		
		if cycleID <= BTMaxCycleID {
			// Likely BTree - verify with meta magic if first page
			if flags&BTPMeta != 0 {
				magic := binary.LittleEndian.Uint32(page[headerSize:])
				if magic == BTMetaMagic {
					return IndexTypeBTree
				}
			} else {
				return IndexTypeBTree
			}
		}
	}
	
	// Check for GIN
	if specialSize >= 8 {
		flags := binary.LittleEndian.Uint16(specialData[6:8])
		// GIN meta page has specific flag pattern
		if flags&GINMeta != 0 || flags&GINData != 0 || flags&GINList != 0 {
			return IndexTypeGIN
		}
	}
	
	return IndexTypeUnknown
}

// parseIndexPage parses a single index page
func parseIndexPage(page []byte, pageNum uint32, indexType IndexType) IndexPageInfo {
	info := IndexPageInfo{
		PageNumber: pageNum,
		IndexType:  indexType,
		TypeString: indexType.String(),
	}
	
	if len(page) < PageSize {
		return info
	}
	
	// Parse common header fields
	info.LSN = binary.LittleEndian.Uint64(page[0:8])
	info.LSNStr = FormatLSN(info.LSN)
	
	lower := binary.LittleEndian.Uint16(page[12:14])
	upper := binary.LittleEndian.Uint16(page[14:16])
	special := binary.LittleEndian.Uint16(page[16:18])
	
	info.FreeSpace = int(upper) - int(lower)
	info.ItemCount = (int(lower) - headerSize) / itemIDSize
	
	// Parse type-specific special section
	if int(special) < PageSize {
		specialData := page[special:]
		
		switch indexType {
		case IndexTypeBTree:
			parseBTreePageSpecial(&info, specialData)
		case IndexTypeHash:
			parseHashPageSpecial(&info, specialData)
		case IndexTypeGiST:
			parseGiSTPageSpecial(&info, specialData)
		case IndexTypeGIN:
			parseGINPageSpecial(&info, specialData)
		case IndexTypeSPGiST:
			parseSPGiSTPageSpecial(&info, specialData)
		}
	}
	
	return info
}

// parseBTreePageSpecial parses BTree special section
func parseBTreePageSpecial(info *IndexPageInfo, special []byte) {
	if len(special) < 16 {
		return
	}
	
	info.PrevBlock = binary.LittleEndian.Uint32(special[0:4])
	info.NextBlock = binary.LittleEndian.Uint32(special[4:8])
	info.Level = binary.LittleEndian.Uint32(special[8:12])
	info.Flags = binary.LittleEndian.Uint16(special[12:14])
	
	info.IsLeaf = info.Flags&BTPLeaf != 0
	info.IsRoot = info.Flags&BTPRoot != 0
	info.IsMeta = info.Flags&BTPMeta != 0
	info.IsDeleted = info.Flags&BTPDeleted != 0
	
	if info.Flags&BTPLeaf != 0 {
		info.FlagStrings = append(info.FlagStrings, "LEAF")
	}
	if info.Flags&BTPRoot != 0 {
		info.FlagStrings = append(info.FlagStrings, "ROOT")
	}
	if info.Flags&BTPDeleted != 0 {
		info.FlagStrings = append(info.FlagStrings, "DELETED")
	}
	if info.Flags&BTPMeta != 0 {
		info.FlagStrings = append(info.FlagStrings, "META")
	}
	if info.Flags&BTPHalfDead != 0 {
		info.FlagStrings = append(info.FlagStrings, "HALF_DEAD")
	}
	if info.Flags&BTPHasGarbage != 0 {
		info.FlagStrings = append(info.FlagStrings, "HAS_GARBAGE")
	}
}

// parseHashPageSpecial parses Hash index special section
func parseHashPageSpecial(info *IndexPageInfo, special []byte) {
	if len(special) < 12 {
		return
	}
	
	info.PrevBlock = binary.LittleEndian.Uint32(special[0:4])
	info.NextBlock = binary.LittleEndian.Uint32(special[4:8])
	bucket := binary.LittleEndian.Uint32(special[8:12])
	info.Flags = binary.LittleEndian.Uint16(special[12:14])
	
	info.IsMeta = info.Flags&LHMeta != 0
	
	if info.Flags&LHBucket != 0 {
		info.FlagStrings = append(info.FlagStrings, "BUCKET")
		info.Level = bucket
	}
	if info.Flags&LHOverflow != 0 {
		info.FlagStrings = append(info.FlagStrings, "OVERFLOW")
	}
	if info.Flags&LHBitmap != 0 {
		info.FlagStrings = append(info.FlagStrings, "BITMAP")
	}
	if info.Flags&LHMeta != 0 {
		info.FlagStrings = append(info.FlagStrings, "META")
	}
}

// parseGiSTPageSpecial parses GiST index special section
func parseGiSTPageSpecial(info *IndexPageInfo, special []byte) {
	if len(special) < 16 {
		return
	}
	
	// NSN (8 bytes) + rightlink (4) + flags (2)
	info.RightLink = binary.LittleEndian.Uint32(special[8:12])
	info.Flags = binary.LittleEndian.Uint16(special[12:14])
	
	info.IsLeaf = info.Flags&FLeaf != 0
	info.IsDeleted = info.Flags&FDeleted != 0
	
	if info.Flags&FLeaf != 0 {
		info.FlagStrings = append(info.FlagStrings, "LEAF")
	}
	if info.Flags&FDeleted != 0 {
		info.FlagStrings = append(info.FlagStrings, "DELETED")
	}
	if info.Flags&FTuplesDeleted != 0 {
		info.FlagStrings = append(info.FlagStrings, "TUPLES_DELETED")
	}
	if info.Flags&FFollowRight != 0 {
		info.FlagStrings = append(info.FlagStrings, "FOLLOW_RIGHT")
	}
}

// parseGINPageSpecial parses GIN index special section
func parseGINPageSpecial(info *IndexPageInfo, special []byte) {
	if len(special) < 8 {
		return
	}
	
	info.RightLink = binary.LittleEndian.Uint32(special[0:4])
	maxOff := binary.LittleEndian.Uint16(special[4:6])
	info.Flags = binary.LittleEndian.Uint16(special[6:8])
	
	info.ItemCount = int(maxOff)
	info.IsLeaf = info.Flags&GINLeaf != 0
	info.IsMeta = info.Flags&GINMeta != 0
	info.IsDeleted = info.Flags&GINDeleted != 0
	
	if info.Flags&GINData != 0 {
		info.FlagStrings = append(info.FlagStrings, "DATA")
	}
	if info.Flags&GINLeaf != 0 {
		info.FlagStrings = append(info.FlagStrings, "LEAF")
	}
	if info.Flags&GINDeleted != 0 {
		info.FlagStrings = append(info.FlagStrings, "DELETED")
	}
	if info.Flags&GINMeta != 0 {
		info.FlagStrings = append(info.FlagStrings, "META")
	}
	if info.Flags&GINList != 0 {
		info.FlagStrings = append(info.FlagStrings, "LIST")
	}
	if info.Flags&GINCompressed != 0 {
		info.FlagStrings = append(info.FlagStrings, "COMPRESSED")
	}
}

// parseSPGiSTPageSpecial parses SP-GiST index special section
func parseSPGiSTPageSpecial(info *IndexPageInfo, special []byte) {
	if len(special) < 6 {
		return
	}
	
	info.Flags = binary.LittleEndian.Uint16(special[0:2])
	// nRedirection + nPlaceholder follow
	
	info.IsLeaf = info.Flags&SPGISTLeaf != 0
	info.IsMeta = info.Flags&SPGISTMeta != 0
	info.IsDeleted = info.Flags&SPGISTDeleted != 0
	
	if info.Flags&SPGISTMeta != 0 {
		info.FlagStrings = append(info.FlagStrings, "META")
	}
	if info.Flags&SPGISTLeaf != 0 {
		info.FlagStrings = append(info.FlagStrings, "LEAF")
	}
	if info.Flags&SPGISTDeleted != 0 {
		info.FlagStrings = append(info.FlagStrings, "DELETED")
	}
	if info.Flags&SPGISTNulls != 0 {
		info.FlagStrings = append(info.FlagStrings, "NULLS")
	}
}

// parseBTreeMeta parses BTree metapage
func parseBTreeMeta(page []byte) *BTreeMetaPage {
	if len(page) < PageSize {
		return nil
	}
	
	// Check if this is a meta page via special section
	special := binary.LittleEndian.Uint16(page[16:18])
	if int(special) >= PageSize {
		return nil
	}
	
	flags := binary.LittleEndian.Uint16(page[special+12 : special+14])
	if flags&BTPMeta == 0 {
		return nil
	}
	
	// Meta data starts after page header
	data := page[headerSize:]
	
	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != BTMetaMagic {
		return nil
	}
	
	return &BTreeMetaPage{
		Magic:     magic,
		Version:   binary.LittleEndian.Uint32(data[4:8]),
		Root:      binary.LittleEndian.Uint32(data[8:12]),
		Level:     binary.LittleEndian.Uint32(data[12:16]),
		FastRoot:  binary.LittleEndian.Uint32(data[16:20]),
		FastLevel: binary.LittleEndian.Uint32(data[20:24]),
	}
}

// parseHashMeta parses Hash index metapage
func parseHashMeta(page []byte) *HashMetaPage {
	if len(page) < PageSize {
		return nil
	}
	
	special := binary.LittleEndian.Uint16(page[16:18])
	if int(special) >= PageSize {
		return nil
	}
	
	flags := binary.LittleEndian.Uint16(page[special+12 : special+14])
	if flags&LHMeta == 0 {
		return nil
	}
	
	// Meta data starts after page header
	data := page[headerSize:]
	
	return &HashMetaPage{
		Magic:      binary.LittleEndian.Uint32(data[0:4]),
		Version:    binary.LittleEndian.Uint32(data[4:8]),
		NumBuckets: binary.LittleEndian.Uint32(data[16:20]),
		MaxBucket:  binary.LittleEndian.Uint32(data[8:12]),
		HighMask:   binary.LittleEndian.Uint32(data[12:16]),
		LowMask:    binary.LittleEndian.Uint32(data[20:24]),
		FFactor:    binary.LittleEndian.Uint16(data[24:26]),
	}
}

// parseGINMeta parses GIN index metapage
func parseGINMeta(page []byte) *GINMetaPage {
	if len(page) < PageSize {
		return nil
	}
	
	special := binary.LittleEndian.Uint16(page[16:18])
	if int(special) >= PageSize {
		return nil
	}
	
	specialData := page[special:]
	if len(specialData) < 8 {
		return nil
	}
	
	flags := binary.LittleEndian.Uint16(specialData[6:8])
	if flags&GINMeta == 0 {
		return nil
	}
	
	// Meta data starts after page header
	data := page[headerSize:]
	
	return &GINMetaPage{
		Version:           binary.LittleEndian.Uint32(data[0:4]),
		Head:              binary.LittleEndian.Uint32(data[4:8]),
		Tail:              binary.LittleEndian.Uint32(data[8:12]),
		TailFreeSize:      binary.LittleEndian.Uint32(data[12:16]),
		NPendingPages:     binary.LittleEndian.Uint32(data[16:20]),
		NPendingHeapTuples: binary.LittleEndian.Uint64(data[24:32]),
		NTotalPages:       binary.LittleEndian.Uint32(data[32:36]),
		NEntryPages:       binary.LittleEndian.Uint32(data[36:40]),
		NDataPages:        binary.LittleEndian.Uint32(data[40:44]),
		NEntries:          binary.LittleEndian.Uint64(data[48:56]),
	}
}
