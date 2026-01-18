package pgdump

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

// === Password Extraction Tests ===

func TestParsePGAuthID(t *testing.T) {
	// Empty data should return empty slice
	results := ParsePGAuthID([]byte{})
	if len(results) != 0 {
		t.Errorf("Expected 0 results for empty data, got %d", len(results))
	}
}

func TestAuthInfoFields(t *testing.T) {
	info := AuthInfo{
		OID:      10,
		RoleName: "postgres",
		Password: "SCRAM-SHA-256$4096:abc123",
		RolSuper: true,
		RolLogin: true,
	}

	if info.RoleName != "postgres" {
		t.Errorf("RoleName = %s, want postgres", info.RoleName)
	}
	if !info.RolSuper {
		t.Error("Expected RolSuper to be true")
	}
}

// === Deleted Rows Tests ===

func TestReadDeletedRowsEmpty(t *testing.T) {
	deleted := ReadDeletedRows([]byte{}, nil)
	if len(deleted) != 0 {
		t.Errorf("Expected 0 deleted rows for empty data, got %d", len(deleted))
	}
}

func TestReadRowsWithDeleted(t *testing.T) {
	// Test with empty data
	visible, deleted := ReadRowsWithDeleted([]byte{}, nil)
	if len(visible) != 0 || len(deleted) != 0 {
		t.Errorf("Expected 0 rows for empty data")
	}
}

// === TOAST Tests ===

func TestIsTOASTPointer(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		isTOAST bool
	}{
		{"empty", []byte{}, false},
		{"short", []byte{0x05}, false},
		{"external", []byte{0x01, 0x00}, true},
		{"compressed external", []byte{0x02, 0x00}, true},
		{"normal varlena", []byte{0x05, 'h', 'i'}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTOASTPointer(tt.data); got != tt.isTOAST {
				t.Errorf("IsTOASTPointer() = %v, want %v", got, tt.isTOAST)
			}
		})
	}
}

func TestParseTOASTPointer(t *testing.T) {
	// Too short
	if ptr := ParseTOASTPointer([]byte{0x01}); ptr != nil {
		t.Error("Expected nil for too short data")
	}

	// Valid pointer (minimal)
	data := make([]byte, 20)
	data[0] = 0x01 // external
	ptr := ParseTOASTPointer(data)
	if ptr == nil {
		t.Error("Expected non-nil pointer")
	}
}

func TestReadTOASTTable(t *testing.T) {
	// Empty data
	chunks := ReadTOASTTable([]byte{})
	if len(chunks) != 0 {
		t.Errorf("Expected 0 chunks for empty data, got %d", len(chunks))
	}
}

func TestReassembleTOAST(t *testing.T) {
	chunks := []TOASTChunk{
		{ChunkID: 1, ChunkSeq: 0, Data: []byte("hello")},
		{ChunkID: 1, ChunkSeq: 1, Data: []byte(" world")},
		{ChunkID: 2, ChunkSeq: 0, Data: []byte("other")},
	}

	// Reassemble value 1
	result := ReassembleTOAST(chunks, 1, false, 11)
	if string(result) != "hello world" {
		t.Errorf("ReassembleTOAST = %q, want 'hello world'", string(result))
	}

	// Non-existent value
	result = ReassembleTOAST(chunks, 99, false, 0)
	if result != nil {
		t.Error("Expected nil for non-existent value")
	}
}

func TestNewTOASTReader(t *testing.T) {
	reader := NewTOASTReader()
	if reader == nil {
		t.Fatal("Expected non-nil reader")
	}
	if reader.chunks == nil {
		t.Error("Expected initialized chunks map")
	}
}

// === Search Tests ===

func TestMatchValue(t *testing.T) {
	re := compilePattern("test")

	tests := []struct {
		name  string
		value interface{}
		match bool
	}{
		{"nil", nil, false},
		{"string match", "this is a test", true},
		{"string no match", "hello world", false},
		{"int", 42, false},
		{"map with match", map[string]interface{}{"key": "test value"}, true},
		{"array with match", []interface{}{"test"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchValue(tt.value, re); got != tt.match {
				t.Errorf("matchValue(%v) = %v, want %v", tt.value, got, tt.match)
			}
		})
	}
}

func compilePattern(pattern string) *regexp.Regexp {
	re, _ := regexp.Compile("(?i)" + pattern)
	return re
}

func TestSearchInDump(t *testing.T) {
	result := &DumpResult{
		Databases: []DatabaseDump{
			{
				Name: "testdb",
				Tables: []TableDump{
					{
						Name: "secrets",
						Columns: []ColumnInfo{
							{Name: "key", TypID: OidText},
							{Name: "value", TypID: OidText},
						},
						Rows: []map[string]interface{}{
							{"key": "api_key", "value": "sk_live_abc123"},
							{"key": "name", "value": "test"},
						},
					},
				},
			},
		},
	}

	matches, err := SearchInDump(result, &SearchOptions{
		Pattern:    "api_key|sk_live",
		IncludeRow: true,
	})
	if err != nil {
		t.Fatalf("SearchInDump failed: %v", err)
	}

	if len(matches) != 2 {
		t.Errorf("Expected 2 matches, got %d", len(matches))
	}
}

// === CSV Tests ===

func TestTableToCSV(t *testing.T) {
	table := TableDump{
		Name: "test",
		Columns: []ColumnInfo{
			{Name: "id", TypID: OidInt4},
			{Name: "name", TypID: OidText},
		},
		Rows: []map[string]interface{}{
			{"id": int32(1), "name": "alice"},
			{"id": int32(2), "name": "bob"},
		},
	}

	var buf bytes.Buffer
	err := table.ToCSV(&buf)
	if err != nil {
		t.Fatalf("ToCSV failed: %v", err)
	}

	csv := buf.String()
	if !strings.Contains(csv, "id,name") {
		t.Error("Missing CSV header")
	}
	if !strings.Contains(csv, "1,alice") {
		t.Error("Missing first row")
	}
	if !strings.Contains(csv, "2,bob") {
		t.Error("Missing second row")
	}
}

func TestFormatCSVValue(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  string
	}{
		{"nil", nil, ""},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"int", int32(42), "42"},
		{"string", "hello", "hello"},
		{"array", []interface{}{1, 2}, "[1,2]"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatCSVValue(tt.value); got != tt.want {
				t.Errorf("formatCSVValue(%v) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

// === WAL Tests ===

func TestRmgrName(t *testing.T) {
	tests := []struct {
		rmid uint8
		want string
	}{
		{RM_HEAP_ID, "Heap"},
		{RM_XACT_ID, "Transaction"},
		{RM_BTREE_ID, "BTree"},
		{255, "RM_255"},
	}

	for _, tt := range tests {
		if got := rmgrName(tt.rmid); got != tt.want {
			t.Errorf("rmgrName(%d) = %s, want %s", tt.rmid, got, tt.want)
		}
	}
}

func TestOperationName(t *testing.T) {
	tests := []struct {
		rmid uint8
		info uint8
		want string
	}{
		{RM_HEAP_ID, 0x00, "INSERT"},
		{RM_HEAP_ID, 0x10, "DELETE"},
		{RM_HEAP_ID, 0x20, "UPDATE"},
		{RM_XACT_ID, 0x00, "COMMIT"},
		{RM_XACT_ID, 0x20, "ABORT"},
	}

	for _, tt := range tests {
		if got := operationName(tt.rmid, tt.info); got != tt.want {
			t.Errorf("operationName(%d, %02x) = %s, want %s", tt.rmid, tt.info, got, tt.want)
		}
	}
}

func TestParseWALFileEmpty(t *testing.T) {
	records, err := ParseWALFile([]byte{})
	if err == nil {
		t.Error("Expected error for empty data")
	}
	if len(records) != 0 {
		t.Errorf("Expected 0 records for empty data, got %d", len(records))
	}
}

func TestParsePageHeader(t *testing.T) {
	// Create minimal page header
	data := make([]byte, 40)
	// Magic (PG16)
	data[0] = 0x13
	data[1] = 0xD1
	// Info (long header)
	data[2] = 0x02
	data[3] = 0x00
	// Timeline
	data[4] = 0x01
	data[5] = 0x00
	data[6] = 0x00
	data[7] = 0x00

	header := parsePageHeader(data)
	if header.Magic != WAL_MAGIC_16 {
		t.Errorf("Expected magic 0x%04X, got 0x%04X", WAL_MAGIC_16, header.Magic)
	}
	if header.TimelineID != 1 {
		t.Errorf("Expected timeline 1, got %d", header.TimelineID)
	}
	if header.Info&XLP_LONG_HEADER == 0 {
		t.Error("Expected long header flag")
	}
}

func TestIsValidMagic(t *testing.T) {
	tests := []struct {
		magic uint16
		valid bool
	}{
		{WAL_MAGIC_16, true},
		{WAL_MAGIC_15, true},
		{WAL_MAGIC_14, true},
		{WAL_MAGIC_13, true},
		{WAL_MAGIC_12, true},
		{0x0000, false},
		{0xFFFF, false},
	}

	for _, tt := range tests {
		if got := isValidMagic(tt.magic); got != tt.valid {
			t.Errorf("isValidMagic(0x%04X) = %v, want %v", tt.magic, got, tt.valid)
		}
	}
}

func TestPgVersionFromMagic(t *testing.T) {
	tests := []struct {
		magic   uint16
		version string
	}{
		{WAL_MAGIC_16, "16"},
		{WAL_MAGIC_15, "15"},
		{WAL_MAGIC_14, "14"},
		{WAL_MAGIC_13, "13"},
		{WAL_MAGIC_12, "12"},
		{0x0000, "unknown"},
	}

	for _, tt := range tests {
		if got := pgVersionFromMagic(tt.magic); got != tt.version {
			t.Errorf("pgVersionFromMagic(0x%04X) = %s, want %s", tt.magic, got, tt.version)
		}
	}
}

func TestFormatLSN(t *testing.T) {
	tests := []struct {
		lsn    uint64
		expect string
	}{
		{0x0000000000000000, "0/0"},
		{0x0000000000001234, "0/1234"},
		{0x0000000100002ABC, "1/2ABC"},
		{0xABCD00001234EFAB, "ABCD0000/1234EFAB"},
	}

	for _, tt := range tests {
		if got := FormatLSN(tt.lsn); got != tt.expect {
			t.Errorf("FormatLSN(0x%016X) = %s, want %s", tt.lsn, got, tt.expect)
		}
	}
}

func TestAlign8(t *testing.T) {
	tests := []struct {
		n    int
		want int
	}{
		{0, 0},
		{1, 8},
		{7, 8},
		{8, 8},
		{9, 16},
		{16, 16},
	}

	for _, tt := range tests {
		if got := align8(tt.n); got != tt.want {
			t.Errorf("align8(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestIsZeroPadding(t *testing.T) {
	if !isZeroPadding([]byte{0, 0, 0, 0, 0, 0, 0, 0}) {
		t.Error("Expected true for all zeros")
	}
	if isZeroPadding([]byte{0, 0, 0, 0, 0, 0, 0, 1}) {
		t.Error("Expected false for non-zero byte")
	}
	if !isZeroPadding([]byte{0, 0}) {
		t.Error("Expected true for short zero slice")
	}
}

func TestParseBlockRefs(t *testing.T) {
	// Empty data
	blocks := parseBlockRefs([]byte{})
	if len(blocks) != 0 {
		t.Errorf("Expected 0 blocks for empty data, got %d", len(blocks))
	}

	// End marker
	blocks = parseBlockRefs([]byte{0xFF})
	if len(blocks) != 0 {
		t.Errorf("Expected 0 blocks for end marker, got %d", len(blocks))
	}

	// Invalid block ID
	blocks = parseBlockRefs([]byte{0x50})
	if len(blocks) != 0 {
		t.Errorf("Expected 0 blocks for invalid block ID, got %d", len(blocks))
	}
}

func TestWALSummaryFields(t *testing.T) {
	summary := WALSummary{
		SegmentCount: 2,
		RecordCount:  100,
		Operations:   map[string]int{"INSERT": 50, "UPDATE": 30, "DELETE": 20},
	}

	if summary.SegmentCount != 2 {
		t.Errorf("SegmentCount = %d, want 2", summary.SegmentCount)
	}
	if summary.Operations["INSERT"] != 50 {
		t.Error("Missing INSERT operations")
	}
}
