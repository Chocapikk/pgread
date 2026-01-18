package pgdump

import (
	"testing"
)

func TestIndexTypeString(t *testing.T) {
	tests := []struct {
		indexType IndexType
		want      string
	}{
		{IndexTypeUnknown, "unknown"},
		{IndexTypeBTree, "btree"},
		{IndexTypeHash, "hash"},
		{IndexTypeGiST, "gist"},
		{IndexTypeGIN, "gin"},
		{IndexTypeSPGiST, "spgist"},
		{IndexTypeBRIN, "brin"},
	}
	
	for _, tt := range tests {
		if got := tt.indexType.String(); got != tt.want {
			t.Errorf("IndexType(%d).String() = %q, want %q", tt.indexType, got, tt.want)
		}
	}
}

func TestDetectIndexTypeBTree(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set up page header
	putU16(page, 12, 28)          // pd_lower
	putU16(page, 14, 8000)        // pd_upper
	putU16(page, 16, 8176)        // pd_special (16 bytes from end)
	putU16(page, 18, 8192|4)      // page size + version
	
	// Set up BTree special section (at offset 8176)
	special := page[8176:]
	putU32(special, 0, 0)         // btpo_prev
	putU32(special, 4, 0)         // btpo_next
	putU32(special, 8, 0)         // level
	putU16(special, 12, BTPMeta)  // flags - META page
	putU16(special, 14, 0)        // cycle_id
	
	// Set up BTree meta data
	meta := page[headerSize:]
	putU32(meta, 0, BTMetaMagic)  // magic
	putU32(meta, 4, 4)            // version
	putU32(meta, 8, 1)            // root
	putU32(meta, 12, 0)           // level
	
	indexType := detectIndexType(page)
	if indexType != IndexTypeBTree {
		t.Errorf("detectIndexType = %v, want BTree", indexType)
	}
}

func TestDetectIndexTypeHash(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set up page header
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	putU16(page, 16, 8176)
	putU16(page, 18, 8192|4)
	
	// Put hash page ID at end of page
	putU16(page, PageSize-2, HashoPageID)
	
	indexType := detectIndexType(page)
	if indexType != IndexTypeHash {
		t.Errorf("detectIndexType = %v, want Hash", indexType)
	}
}

func TestDetectIndexTypeGiST(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set up page header
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	putU16(page, 16, 8176)
	putU16(page, 18, 8192|4)
	
	// Put GiST page ID at end of page
	putU16(page, PageSize-2, GISTPageID)
	
	indexType := detectIndexType(page)
	if indexType != IndexTypeGiST {
		t.Errorf("detectIndexType = %v, want GiST", indexType)
	}
}

func TestDetectIndexTypeSPGiST(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set up page header
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	putU16(page, 16, 8176)
	putU16(page, 18, 8192|4)
	
	// Put SP-GiST page ID at end of page
	putU16(page, PageSize-2, SPGISTPageID)
	
	indexType := detectIndexType(page)
	if indexType != IndexTypeSPGiST {
		t.Errorf("detectIndexType = %v, want SP-GiST", indexType)
	}
}

func TestParseBTreePageSpecial(t *testing.T) {
	special := make([]byte, 16)
	putU32(special, 0, 5)                      // prev
	putU32(special, 4, 10)                     // next
	putU32(special, 8, 2)                      // level
	putU16(special, 12, BTPLeaf|BTPHasGarbage) // flags
	
	info := &IndexPageInfo{}
	parseBTreePageSpecial(info, special)
	
	if info.PrevBlock != 5 {
		t.Errorf("PrevBlock = %d, want 5", info.PrevBlock)
	}
	if info.NextBlock != 10 {
		t.Errorf("NextBlock = %d, want 10", info.NextBlock)
	}
	if info.Level != 2 {
		t.Errorf("Level = %d, want 2", info.Level)
	}
	if !info.IsLeaf {
		t.Error("IsLeaf should be true")
	}
	if info.IsRoot {
		t.Error("IsRoot should be false")
	}
	
	// Check flag strings
	found := make(map[string]bool)
	for _, f := range info.FlagStrings {
		found[f] = true
	}
	if !found["LEAF"] {
		t.Error("Missing LEAF flag string")
	}
	if !found["HAS_GARBAGE"] {
		t.Error("Missing HAS_GARBAGE flag string")
	}
}

func TestParseHashPageSpecial(t *testing.T) {
	special := make([]byte, 16)
	putU32(special, 0, 3)         // prev
	putU32(special, 4, 7)         // next
	putU32(special, 8, 42)        // bucket
	putU16(special, 12, LHBucket) // flags
	
	info := &IndexPageInfo{}
	parseHashPageSpecial(info, special)
	
	if info.PrevBlock != 3 {
		t.Errorf("PrevBlock = %d, want 3", info.PrevBlock)
	}
	if info.NextBlock != 7 {
		t.Errorf("NextBlock = %d, want 7", info.NextBlock)
	}
	if info.Level != 42 { // bucket number stored in Level
		t.Errorf("Level (bucket) = %d, want 42", info.Level)
	}
}

func TestParseGiSTPageSpecial(t *testing.T) {
	special := make([]byte, 16)
	putU64(special, 0, 0x12345678) // NSN
	putU32(special, 8, 15)         // rightlink
	putU16(special, 12, FLeaf|FDeleted)
	
	info := &IndexPageInfo{}
	parseGiSTPageSpecial(info, special)
	
	if info.RightLink != 15 {
		t.Errorf("RightLink = %d, want 15", info.RightLink)
	}
	if !info.IsLeaf {
		t.Error("IsLeaf should be true")
	}
	if !info.IsDeleted {
		t.Error("IsDeleted should be true")
	}
}

func TestParseGINPageSpecial(t *testing.T) {
	special := make([]byte, 8)
	putU32(special, 0, 20)                   // rightlink
	putU16(special, 4, 100)                  // maxoff
	putU16(special, 6, GINLeaf|GINCompressed)
	
	info := &IndexPageInfo{}
	parseGINPageSpecial(info, special)
	
	if info.RightLink != 20 {
		t.Errorf("RightLink = %d, want 20", info.RightLink)
	}
	if info.ItemCount != 100 {
		t.Errorf("ItemCount = %d, want 100", info.ItemCount)
	}
	if !info.IsLeaf {
		t.Error("IsLeaf should be true")
	}
}

func TestParseSPGiSTPageSpecial(t *testing.T) {
	special := make([]byte, 8)
	putU16(special, 0, SPGISTLeaf|SPGISTNulls)
	
	info := &IndexPageInfo{}
	parseSPGiSTPageSpecial(info, special)
	
	if !info.IsLeaf {
		t.Error("IsLeaf should be true")
	}
	if info.IsMeta {
		t.Error("IsMeta should be false")
	}
}

func TestParseBTreeMeta(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set up page header
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	putU16(page, 16, 8176)
	putU16(page, 18, 8192|4)
	
	// Set up special section with META flag
	special := page[8176:]
	putU16(special, 12, BTPMeta)
	
	// Set up meta data
	meta := page[headerSize:]
	putU32(meta, 0, BTMetaMagic)
	putU32(meta, 4, 4)   // version
	putU32(meta, 8, 3)   // root
	putU32(meta, 12, 2)  // level
	putU32(meta, 16, 3)  // fastroot
	putU32(meta, 20, 1)  // fastlevel
	
	result := parseBTreeMeta(page)
	if result == nil {
		t.Fatal("parseBTreeMeta returned nil")
	}
	
	if result.Magic != BTMetaMagic {
		t.Errorf("Magic = 0x%X, want 0x%X", result.Magic, BTMetaMagic)
	}
	if result.Version != 4 {
		t.Errorf("Version = %d, want 4", result.Version)
	}
	if result.Root != 3 {
		t.Errorf("Root = %d, want 3", result.Root)
	}
	if result.Level != 2 {
		t.Errorf("Level = %d, want 2", result.Level)
	}
}

func TestParseIndexFile(t *testing.T) {
	// Create a minimal BTree index file
	data := make([]byte, PageSize*2)
	
	// Page 0: Meta page
	page0 := data[0:PageSize]
	putU16(page0, 12, 28)
	putU16(page0, 14, 8000)
	putU16(page0, 16, 8176)
	putU16(page0, 18, 8192|4)
	special0 := page0[8176:]
	putU16(special0, 12, BTPMeta)
	meta := page0[headerSize:]
	putU32(meta, 0, BTMetaMagic)
	putU32(meta, 4, 4)
	putU32(meta, 8, 1) // root at page 1
	
	// Page 1: Root page
	page1 := data[PageSize : PageSize*2]
	putU16(page1, 12, 28)
	putU16(page1, 14, 8000)
	putU16(page1, 16, 8176)
	putU16(page1, 18, 8192|4)
	special1 := page1[8176:]
	putU16(special1, 12, BTPRoot|BTPLeaf)
	
	info, err := ParseIndexFile(data)
	if err != nil {
		t.Fatalf("ParseIndexFile failed: %v", err)
	}
	
	if info.Type != IndexTypeBTree {
		t.Errorf("Type = %v, want BTree", info.Type)
	}
	if info.TotalPages != 2 {
		t.Errorf("TotalPages = %d, want 2", info.TotalPages)
	}
	if info.RootPage != 1 {
		t.Errorf("RootPage = %d, want 1", info.RootPage)
	}
}

func TestParseIndexFileTooSmall(t *testing.T) {
	data := make([]byte, 100)
	_, err := ParseIndexFile(data)
	if err == nil {
		t.Error("expected error for small file")
	}
}

// putU32 and putU64 are defined in control_test.go
