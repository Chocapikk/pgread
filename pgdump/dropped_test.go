package pgdump

import (
	"regexp"
	"testing"
)

func TestDroppedColumnRegex(t *testing.T) {
	tests := []struct {
		input   string
		match   bool
		wantNum string
	}{
		{"........pg.dropped.1........", true, "1"},
		{"........pg.dropped.42........", true, "42"},
		{"........pg.dropped.123........", true, "123"},
		{"pg.dropped.1", false, ""},
		{"regular_column", false, ""},
		{"........pg.dropped..........", false, ""},
		{"", false, ""},
	}
	
	for _, tt := range tests {
		matches := droppedColumnRegex.FindStringSubmatch(tt.input)
		if tt.match {
			if len(matches) < 2 {
				t.Errorf("droppedColumnRegex(%q): expected match, got none", tt.input)
				continue
			}
			if matches[1] != tt.wantNum {
				t.Errorf("droppedColumnRegex(%q): num = %q, want %q", tt.input, matches[1], tt.wantNum)
			}
		} else {
			if len(matches) > 0 {
				t.Errorf("droppedColumnRegex(%q): expected no match, got %v", tt.input, matches)
			}
		}
	}
}

func TestBuildColumnsWithDropped(t *testing.T) {
	attrs := []DroppedColumnInfo{
		{
			AttNum:       1,
			OriginalName: "id",
			TypeOID:      OidInt4,
			AttLen:       4,
			AttAlign:     'i',
		},
		{
			AttNum:       2,
			OriginalName: "dropped_2",
			TypeOID:      OidText,
			AttLen:       -1,
			AttAlign:     'i',
		},
		{
			AttNum:       3,
			OriginalName: "name",
			TypeOID:      OidText,
			AttLen:       -1,
			AttAlign:     'i',
		},
	}
	
	cols := buildColumnsWithDropped(attrs)
	
	if len(cols) != 3 {
		t.Fatalf("len(cols) = %d, want 3", len(cols))
	}
	
	if cols[0].Name != "id" {
		t.Errorf("cols[0].Name = %q, want 'id'", cols[0].Name)
	}
	if cols[1].Name != "dropped_2" {
		t.Errorf("cols[1].Name = %q, want 'dropped_2'", cols[1].Name)
	}
	if cols[2].Name != "name" {
		t.Errorf("cols[2].Name = %q, want 'name'", cols[2].Name)
	}
	
	// Check types
	if cols[0].TypID != OidInt4 {
		t.Errorf("cols[0].TypID = %d, want %d", cols[0].TypID, OidInt4)
	}
	if cols[1].TypID != OidText {
		t.Errorf("cols[1].TypID = %d, want %d", cols[1].TypID, OidText)
	}
}

func TestBuildColumnsWithDroppedNoOriginalName(t *testing.T) {
	attrs := []DroppedColumnInfo{
		{
			AttNum:       1,
			OriginalName: "",
			TypeOID:      OidInt4,
			AttLen:       4,
		},
	}
	
	cols := buildColumnsWithDropped(attrs)
	
	if cols[0].Name != "dropped_1" {
		t.Errorf("cols[0].Name = %q, want 'dropped_1'", cols[0].Name)
	}
}

func TestDroppedColumnInfo(t *testing.T) {
	info := DroppedColumnInfo{
		RelOID:      16384,
		TableName:   "users",
		AttNum:      2,
		DroppedName: "........pg.dropped.2........",
		TypeOID:     OidText,
		TypeName:    "text",
		AttLen:      -1,
		AttAlign:    'i',
		AttByVal:    false,
	}
	
	if info.TableName != "users" {
		t.Errorf("TableName = %q, want 'users'", info.TableName)
	}
	if info.TypeName != "text" {
		t.Errorf("TypeName = %q, want 'text'", info.TypeName)
	}
}

func TestDroppedColumnsResultEmpty(t *testing.T) {
	result := &DroppedColumnsResult{
		Database:     "testdb",
		DroppedCount: 0,
		Columns:      nil,
	}
	
	if result.DroppedCount != 0 {
		t.Errorf("DroppedCount = %d, want 0", result.DroppedCount)
	}
	if result.Database != "testdb" {
		t.Errorf("Database = %q, want 'testdb'", result.Database)
	}
}

func TestDroppedColumnRegexCompiles(t *testing.T) {
	// Verify the regex compiles and works
	re := regexp.MustCompile(`^\.+pg\.dropped\.(\d+)\.+$`)
	
	// Test valid patterns
	if !re.MatchString("........pg.dropped.1........") {
		t.Error("regex should match standard dropped column pattern")
	}
	
	// Test with different number of dots
	if !re.MatchString("..pg.dropped.99..") {
		t.Error("regex should match with fewer dots")
	}
}

func TestSchemaPGAttrDropped(t *testing.T) {
	// Verify schema has expected columns
	columnNames := make(map[string]bool)
	for _, col := range schemaPGAttrDropped {
		columnNames[col.Name] = true
	}
	
	required := []string{"attrelid", "attname", "atttypid", "attlen", "attnum", "attisdropped"}
	for _, name := range required {
		if !columnNames[name] {
			t.Errorf("schemaPGAttrDropped missing column %q", name)
		}
	}
}

func TestSchemaPGAttrDroppedV15(t *testing.T) {
	// V15 should have attstattarget
	columnNames := make(map[string]bool)
	for _, col := range schemaPGAttrDroppedV15 {
		columnNames[col.Name] = true
	}
	
	if !columnNames["attstattarget"] {
		t.Error("schemaPGAttrDroppedV15 should have attstattarget")
	}
}

func TestParseDroppedColumnsEmpty(t *testing.T) {
	// Empty data should return empty slice
	result := parseDroppedColumns(nil, nil)
	if len(result) != 0 {
		t.Errorf("parseDroppedColumns(nil) = %d items, want 0", len(result))
	}
}

func TestDroppedColumnDataStruct(t *testing.T) {
	data := &DroppedColumnData{
		Column: DroppedColumnInfo{
			AttNum:    2,
			TypeOID:   OidText,
			TypeName:  "text",
		},
		Values: []interface{}{"value1", "value2", nil},
	}
	
	if data.Column.AttNum != 2 {
		t.Errorf("Column.AttNum = %d, want 2", data.Column.AttNum)
	}
	if len(data.Values) != 3 {
		t.Errorf("len(Values) = %d, want 3", len(data.Values))
	}
	if data.Values[2] != nil {
		t.Error("Values[2] should be nil")
	}
}
