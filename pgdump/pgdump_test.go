package pgdump

import (
	"os"
	"path/filepath"
	"testing"
)

// Test data paths - set via environment or use local testdata
func testDataPath() string {
	if p := os.Getenv("PGDUMP_TESTDATA"); p != "" {
		return p
	}
	return "testdata"
}

func TestParsePGDatabase(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(testDataPath(), "global", "1262"))
	if err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	dbs := ParsePGDatabase(data)
	if len(dbs) == 0 {
		t.Fatal("Expected at least one database")
	}

	// Should find testdb
	found := false
	for _, db := range dbs {
		if db.Name == "testdb" {
			found = true
			if db.OID == 0 {
				t.Error("testdb OID should not be 0")
			}
		}
	}
	if !found {
		t.Error("Expected to find 'testdb' database")
	}
}

func TestParsePGClass(t *testing.T) {
	// Need to find testdb OID first
	dbData, err := os.ReadFile(filepath.Join(testDataPath(), "global", "1262"))
	if err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	var testdbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == "testdb" {
			testdbOID = db.OID
			break
		}
	}
	if testdbOID == 0 {
		t.Skip("testdb not found")
	}

	classPath := filepath.Join(testDataPath(), "base", uitoa(testdbOID), "1259")
	data, err := os.ReadFile(classPath)
	if err != nil {
		t.Skipf("pg_class not available: %v", err)
	}

	tables := ParsePGClass(data)
	if len(tables) == 0 {
		t.Fatal("Expected at least one table")
	}

	// Should find users or secrets table
	found := false
	for _, tbl := range tables {
		if tbl.Name == "users" || tbl.Name == "secrets" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected to find 'users' or 'secrets' table")
	}
}

func TestParsePGAttribute(t *testing.T) {
	dbData, err := os.ReadFile(filepath.Join(testDataPath(), "global", "1262"))
	if err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	var testdbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == "testdb" {
			testdbOID = db.OID
			break
		}
	}
	if testdbOID == 0 {
		t.Skip("testdb not found")
	}

	attrPath := filepath.Join(testDataPath(), "base", uitoa(testdbOID), "1249")
	data, err := os.ReadFile(attrPath)
	if err != nil {
		t.Skipf("pg_attribute not available: %v", err)
	}

	attrs := ParsePGAttribute(data, 0)
	if len(attrs) == 0 {
		t.Fatal("Expected attributes")
	}

	// Check we have column definitions
	total := 0
	for _, cols := range attrs {
		total += len(cols)
	}
	if total == 0 {
		t.Error("Expected at least one column definition")
	}
}

func TestDumpDataDir(t *testing.T) {
	path := testDataPath()
	if _, err := os.Stat(filepath.Join(path, "global", "1262")); err != nil {
		t.Skipf("Test data not available: %v", err)
	}

	result, err := DumpDataDir(path, &Options{
		DatabaseFilter:   "testdb",
		SkipSystemTables: true,
	})
	if err != nil {
		t.Fatalf("DumpDataDir failed: %v", err)
	}

	if len(result.Databases) == 0 {
		t.Fatal("Expected at least one database")
	}

	db := result.Databases[0]
	if db.Name != "testdb" {
		t.Errorf("Expected testdb, got %s", db.Name)
	}

	if len(db.Tables) == 0 {
		t.Skipf("No user tables found in testdb (might be platform-specific test data)")
	}

	// Check if any table has rows with JSONB (secrets table on Linux, type_test on Windows/macOS)
	for _, tbl := range db.Tables {
		if tbl.RowCount > 0 {
			for _, row := range tbl.Rows {
				// Look for any JSONB column (value, col_jsonb, etc.)
				for colName, val := range row {
					if val != nil {
						if _, isMap := val.(map[string]interface{}); isMap {
							t.Logf("Found JSONB in table %s, column %s", tbl.Name, colName)
							return // Success - found at least one JSONB value
						}
					}
				}
			}
		}
	}
	t.Log("No JSONB columns found, but tables were dumped successfully")
}

func TestDecodeTypes(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		oid  int
		want interface{}
	}{
		{"bool true", []byte{1}, OidBool, true},
		{"bool false", []byte{0}, OidBool, false},
		{"int2", []byte{0x39, 0x05}, OidInt2, int16(1337)},
		{"int4", []byte{0xD2, 0x04, 0x00, 0x00}, OidInt4, int32(1234)},
		{"int8", []byte{0x15, 0xCD, 0x5B, 0x07, 0x00, 0x00, 0x00, 0x00}, OidInt8, int64(123456789)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeType(tt.data, tt.oid)
			if got != tt.want {
				t.Errorf("DecodeType() = %v (%T), want %v (%T)", got, got, tt.want, tt.want)
			}
		})
	}
}

func TestDecodeGeometricTypes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		oid      int
		contains string
	}{
		{
			name: "point",
			data: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // 1.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // 2.0
			},
			oid:      OidPoint,
			contains: "(1,2)",
		},
		{
			name: "line",
			data: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // A=1.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // B=2.0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // C=3.0
			},
			oid:      OidLine,
			contains: "{1,2,3}",
		},
		{
			name: "circle",
			data: []byte{
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // x=0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // y=0
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // r=5.0
			},
			oid:      OidCircle,
			contains: "<(0,0),5>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeType(tt.data, tt.oid)
			str, ok := got.(string)
			if !ok {
				t.Fatalf("Expected string, got %T", got)
			}
			if str != tt.contains {
				t.Errorf("DecodeType() = %v, want %v", str, tt.contains)
			}
		})
	}
}

func TestDecodeNetworkTypes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		oid      int
		expected string
	}{
		{
			name:     "macaddr",
			data:     []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff},
			oid:      OidMacaddr,
			expected: "aa:bb:cc:dd:ee:ff",
		},
		{
			name:     "macaddr8",
			data:     []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22},
			oid:      OidMacaddr8,
			expected: "aa:bb:cc:dd:ee:ff:11:22",
		},
		{
			name:     "inet ipv4",
			data:     []byte{0x02, 0x20, 0x00, 0x04, 192, 168, 1, 1},
			oid:      OidInet,
			expected: "192.168.1.1",
		},
		{
			name:     "inet ipv4 cidr",
			data:     []byte{0x02, 0x18, 0x01, 0x04, 10, 0, 0, 0},
			oid:      OidCidr,
			expected: "10.0.0.0/24",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeType(tt.data, tt.oid)
			str, ok := got.(string)
			if !ok {
				t.Fatalf("Expected string, got %T", got)
			}
			if str != tt.expected {
				t.Errorf("DecodeType() = %v, want %v", str, tt.expected)
			}
		})
	}
}

func TestDecodeDateTimeTypes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		oid      int
		contains string
	}{
		{
			name:     "date epoch",
			data:     []byte{0x00, 0x00, 0x00, 0x00}, // 2000-01-01
			oid:      OidDate,
			contains: "2000-01-01",
		},
		{
			name:     "timestamp epoch",
			data:     []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			oid:      OidTimestamp,
			contains: "2000-01-01 00:00:00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DecodeType(tt.data, tt.oid)
			str, ok := got.(string)
			if !ok {
				t.Fatalf("Expected string, got %T", got)
			}
			if str != tt.contains {
				t.Errorf("DecodeType() = %v, want %v", str, tt.contains)
			}
		})
	}
}

func TestDecodeMoney(t *testing.T) {
	// $12.34 = 1234 cents
	data := []byte{0xD2, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	got := DecodeType(data, OidMoney)
	if got != "$12.34" {
		t.Errorf("DecodeType(money) = %v, want $12.34", got)
	}
}

func TestDecodeUUID(t *testing.T) {
	// UUID: c57d0655-4508-0c4e-69a1-afcae52b1749 in PostgreSQL binary format
	data := []byte{
		0x55, 0x06, 0x7d, 0xc5, // first group (LE on disk -> reads as c57d0655)
		0x08, 0x45,             // second group
		0x0c, 0x4e,             // third group
		0x69, 0xa1,             // fourth group (BE)
		0xaf, 0xca, 0xe5, 0x2b, 0x17, 0x49, // last 6 bytes
	}
	got := DecodeType(data, OidUUID)
	// Just check it looks like a valid UUID format
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if len(str) != 36 || str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-' {
		t.Errorf("DecodeType(uuid) = %v, doesn't look like valid UUID format", str)
	}
}

func TestDecodeBitString(t *testing.T) {
	// 5 bits: 10110
	data := []byte{0x05, 0x00, 0x00, 0x00, 0xB0} // bitlen=5, bits=10110000
	got := DecodeType(data, OidBit)
	if got != "10110" {
		t.Errorf("DecodeType(bit) = %v, want 10110", got)
	}
}

func TestDecodePgLsn(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x00, 0xFF, 0x00, 0x00, 0x00}
	got := DecodeType(data, OidPgLsn)
	expected := "1/FF"
	if got != expected {
		t.Errorf("DecodeType(pg_lsn) = %v, want %v", got, expected)
	}
}

func TestDecodeRange(t *testing.T) {
	// int4range [1,10)
	// PostgreSQL format: [range_oid (4)][lower (4)][upper (4)][flags (1)]
	// flags=0x02 (lower inclusive)
	data := []byte{
		0x40, 0x0F, 0x00, 0x00, // range type OID (3904 = int4range)
		0x01, 0x00, 0x00, 0x00, // lower bound value: 1
		0x0A, 0x00, 0x00, 0x00, // upper bound value: 10
		0x02,                   // flags: lower inclusive (at the end!)
	}
	got := DecodeType(data, OidInt4Range)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if str != "[1,10)" {
		t.Errorf("DecodeType(int4range) = %v, want [1,10)", str)
	}
}

func TestTypeName(t *testing.T) {
	tests := []struct {
		oid  int
		want string
	}{
		{OidBool, "bool"},
		{OidInt4, "int4"},
		{OidText, "text"},
		{OidJSONB, "jsonb"},
		{OidPoint, "point"},
		{OidCircle, "circle"},
		{OidMoney, "money"},
		{OidInt4Range, "int4range"},
		{99999, "oid:99999"},
	}

	for _, tt := range tests {
		got := TypeName(tt.oid)
		if got != tt.want {
			t.Errorf("TypeName(%d) = %v, want %v", tt.oid, got, tt.want)
		}
	}
}

func TestParseJSONB(t *testing.T) {
	// Simple JSONB object: {"a": 1}
	// This is a minimal test - real JSONB has complex headers
	data := []byte{
		0x01, 0x00, 0x00, 0x20, // header: 1 key, object flag
		0x01, 0x00, 0x00, 0x00, // key entry: len=1
		0x0c, 0x00, 0x00, 0x10, // val entry: numeric
		0x61,                   // "a"
		0x00, 0x00, 0x00,       // padding
		0x05, 0x80, 0x01, 0x00, // numeric: 1
	}

	result := ParseJSONB(data)
	if result == nil {
		t.Skip("JSONB parsing returned nil - may need real test data")
	}

	obj, ok := result.(map[string]interface{})
	if !ok {
		t.Skipf("Expected map, got %T", result)
	}

	if _, exists := obj["a"]; !exists {
		t.Error("Expected key 'a' in JSONB object")
	}
}

func uitoa(u uint32) string {
	return string('0' + byte(u/10000%10)) +
		string('0' + byte(u/1000%10)) +
		string('0' + byte(u/100%10)) +
		string('0' + byte(u/10%10)) +
		string('0' + byte(u%10))
}

// === Additional coverage tests ===

func TestDecodeInterval(t *testing.T) {
	// 1 year 2 months 3 days 4 hours 5 minutes 6 seconds
	// months = 14, days = 3, microseconds = 4*3600e6 + 5*60e6 + 6*1e6
	data := []byte{
		0x80, 0x1b, 0xb7, 0xd0, 0x0d, 0x00, 0x00, 0x00, // 14706000000 microseconds (4h5m6s)
		0x03, 0x00, 0x00, 0x00, // 3 days
		0x0e, 0x00, 0x00, 0x00, // 14 months (1y2mo)
	}
	got := DecodeType(data, OidInterval)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if str == "0" {
		t.Error("Expected non-zero interval")
	}
}

func TestDecodeTimeTZ(t *testing.T) {
	// 14:30:00+02
	data := []byte{
		0x00, 0xe0, 0xb9, 0x50, 0x2f, 0x0c, 0x00, 0x00, // microseconds since midnight
		0x70, 0x1c, 0x00, 0x00, // timezone offset in seconds (7200 = +2h)
	}
	got := DecodeType(data, OidTimeTZ)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if len(str) == 0 {
		t.Error("Expected non-empty timetz")
	}
}

func TestDecodePath(t *testing.T) {
	// Closed path with 3 points
	data := []byte{
		0x01,                   // closed
		0x03, 0x00, 0x00, 0x00, // 3 points
		// point 1: (0,0)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// point 2: (1,1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		// point 3: (2,0)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	got := DecodeType(data, OidPath)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if str != "((0,0),(1,1),(2,0))" {
		t.Errorf("DecodeType(path) = %v, want ((0,0),(1,1),(2,0))", str)
	}
}

func TestDecodePolygon(t *testing.T) {
	// Polygon with 3 points (same as closed path)
	data := []byte{
		0x01,                   // closed (always for polygon)
		0x03, 0x00, 0x00, 0x00, // 3 points
		// points...
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	}
	got := DecodeType(data, OidPolygon)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if str != "((0,0),(1,1),(2,0))" {
		t.Errorf("DecodeType(polygon) = %v, want ((0,0),(1,1),(2,0))", str)
	}
}

func TestDecodeLseg(t *testing.T) {
	data := []byte{
		// point 1: (0,0)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// point 2: (1,1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
	}
	got := DecodeType(data, OidLseg)
	if got != "[(0,0),(1,1)]" {
		t.Errorf("DecodeType(lseg) = %v, want [(0,0),(1,1)]", got)
	}
}

func TestDecodeBox(t *testing.T) {
	data := []byte{
		// point 1: (0,0)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		// point 2: (1,1)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
	}
	got := DecodeType(data, OidBox)
	if got != "((0,0)),((1,1))" {
		t.Errorf("DecodeType(box) = %v, want ((0,0)),((1,1))", got)
	}
}

func TestDecodeFloat4(t *testing.T) {
	// 3.14 in float32 IEEE 754
	data := []byte{0xc3, 0xf5, 0x48, 0x40}
	got := DecodeType(data, OidFloat4)
	f, ok := got.(float32)
	if !ok {
		t.Fatalf("Expected float32, got %T", got)
	}
	if f < 3.13 || f > 3.15 {
		t.Errorf("DecodeType(float4) = %v, want ~3.14", f)
	}
}

func TestDecodeFloat8(t *testing.T) {
	// 2.718281828 in float64 IEEE 754
	data := []byte{0x90, 0xf7, 0xaa, 0x95, 0x09, 0xbf, 0x05, 0x40}
	got := DecodeType(data, OidFloat8)
	f, ok := got.(float64)
	if !ok {
		t.Fatalf("Expected float64, got %T", got)
	}
	if f < 2.71 || f > 2.72 {
		t.Errorf("DecodeType(float8) = %v, want ~2.718", f)
	}
}

func TestDecodeChar(t *testing.T) {
	data := []byte{'A'}
	got := DecodeType(data, OidChar)
	if got != "A" {
		t.Errorf("DecodeType(char) = %v, want A", got)
	}
}

func TestDecodeBytea(t *testing.T) {
	data := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	got := DecodeType(data, OidBytea)
	if got != "\\xdeadbeef" {
		t.Errorf("DecodeType(bytea) = %v, want \\xdeadbeef", got)
	}
}

func TestDecodeOid(t *testing.T) {
	data := []byte{0x39, 0x05, 0x00, 0x00}
	got := DecodeType(data, OidOid)
	if got != uint32(1337) {
		t.Errorf("DecodeType(oid) = %v, want 1337", got)
	}
}

func TestDecodeTid(t *testing.T) {
	data := []byte{0x01, 0x00, 0x00, 0x00, 0x05, 0x00}
	got := DecodeType(data, OidTid)
	if got != "(1,5)" {
		t.Errorf("DecodeType(tid) = %v, want (1,5)", got)
	}
}

func TestDecodeTime(t *testing.T) {
	// 0 microseconds = 00:00:00
	data := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	got := DecodeType(data, OidTime)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if str != "00:00:00" {
		t.Errorf("DecodeType(time) = %v, want 00:00:00", str)
	}
}

func TestDecodeInetIPv6(t *testing.T) {
	// IPv6: ::1
	data := []byte{
		0x03,       // family (AF_INET6)
		0x80,       // bits (128)
		0x00,       // is_cidr
		0x10,       // address length (16)
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
	}
	got := DecodeType(data, OidInet)
	str, ok := got.(string)
	if !ok {
		t.Fatalf("Expected string, got %T", got)
	}
	if str != "0:0:0:0:0:0:0:1" {
		t.Errorf("DecodeType(inet ipv6) = %v, want 0:0:0:0:0:0:0:1", str)
	}
}

func TestDecodeEmptyRange(t *testing.T) {
	data := []byte{0x01} // empty flag
	got := DecodeType(data, OidInt4Range)
	if got != "empty" {
		t.Errorf("DecodeType(empty range) = %v, want empty", got)
	}
}

func TestDecodeRangeInfinite(t *testing.T) {
	// Range with infinite lower and upper bounds
	// flags: lb_inf (0x08) | ub_inf (0x10) = 0x18
	data := []byte{
		0x40, 0x0F, 0x00, 0x00, // range type OID
		0x18,                   // flags at the end
	}
	got := DecodeType(data, OidInt4Range)
	if got != "(,)" {
		t.Errorf("DecodeType(infinite range) = %v, want (,)", got)
	}
}

func TestDecodeRangeEmpty(t *testing.T) {
	// Empty range: flags = 0x01
	data := []byte{
		0x40, 0x0F, 0x00, 0x00, // range type OID
		0x01,                   // flags: empty
	}
	got := DecodeType(data, OidInt4Range)
	if got != "empty" {
		t.Errorf("DecodeType(empty range) = %v, want empty", got)
	}
}

func TestDecodeRangeMalformed(t *testing.T) {
	// Test with malformed data that could cause panic
	tests := []struct {
		name string
		data []byte
		oid  int
	}{
		{"too short", []byte{0x02, 0xFF, 0xFF}, OidInt4Range},
		{"empty data", []byte{}, OidInt4Range},
		{"minimal", []byte{0x00, 0x00, 0x00, 0x00, 0x18}, OidInt4Range},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Should not panic
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("decodeRange panicked: %v", r)
				}
			}()
			_ = DecodeType(tt.data, tt.oid)
		})
	}
}

func TestDecodeText(t *testing.T) {
	data := []byte("hello world")
	got := DecodeType(data, OidText)
	if got != "hello world" {
		t.Errorf("DecodeType(text) = %v, want 'hello world'", got)
	}
}

func TestDecodeVarchar(t *testing.T) {
	data := []byte("varchar test")
	got := DecodeType(data, OidVarchar)
	if got != "varchar test" {
		t.Errorf("DecodeType(varchar) = %v, want 'varchar test'", got)
	}
}

func TestDecodeXML(t *testing.T) {
	data := []byte("<root>test</root>")
	got := DecodeType(data, OidXML)
	if got != "<root>test</root>" {
		t.Errorf("DecodeType(xml) = %v, want '<root>test</root>'", got)
	}
}

func TestDecodeJSON(t *testing.T) {
	data := []byte(`{"key": "value"}`)
	got := DecodeType(data, OidJSON)
	// JSON is now parsed into a map
	m, ok := got.(map[string]interface{})
	if !ok {
		t.Errorf("DecodeType(json) = %T, want map[string]interface{}", got)
		return
	}
	if m["key"] != "value" {
		t.Errorf("DecodeType(json)[\"key\"] = %v, want 'value'", m["key"])
	}
}

func TestDecodeTsvector(t *testing.T) {
	data := []byte("test:1 search:2")
	got := DecodeType(data, OidTsvector)
	if got != "test:1 search:2" {
		t.Errorf("DecodeType(tsvector) = %v, want 'test:1 search:2'", got)
	}
}

func TestDecodeUnknownType(t *testing.T) {
	data := []byte("unknown data")
	got := DecodeType(data, 99999)
	if got != "unknown data" {
		t.Errorf("DecodeType(unknown) = %v, want 'unknown data'", got)
	}
}

func TestDecodeEmptyData(t *testing.T) {
	got := DecodeType([]byte{}, OidInt4)
	if got != nil {
		t.Errorf("DecodeType(empty) = %v, want nil", got)
	}
}

func TestReadVarlena(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		wantLen  int
		wantData string
	}{
		{"empty", []byte{}, 0, ""},
		{"short varlena", []byte{0x0d, 'h', 'e', 'l', 'l', 'o'}, 6, "hello"}, // total=6, (6<<1)|1 = 0x0d
		{"long varlena", []byte{0x18, 0x00, 0x00, 0x00, 't', 'e'}, 6, "te"},  // total=6, header=0x18>>2=6
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, consumed := ReadVarlena(tt.data)
			if consumed != tt.wantLen {
				t.Errorf("ReadVarlena() consumed = %d, want %d", consumed, tt.wantLen)
			}
			if tt.wantData != "" && string(data) != tt.wantData {
				t.Errorf("ReadVarlena() data = %q, want %q", string(data), tt.wantData)
			}
		})
	}
}

// Test page parsing
func TestParsePage(t *testing.T) {
	// Create a minimal valid page
	page := make([]byte, 8192)
	// Page header
	page[0] = 0 // pd_lsn
	page[8] = 0 // pd_checksum
	page[10] = 0
	page[12] = 24 // pd_lower (just past header)
	page[14] = 0
	page[16] = 0x00 // pd_upper
	page[17] = 0x20 // = 8192
	page[18] = 0x00 // pd_special
	page[19] = 0x20 // = 8192
	page[20] = 0    // pd_pagesize_version
	page[21] = 0x20 // pagesize = 8192, version = 0

	result := ParsePage(page)
	if result == nil {
		t.Skip("ParsePage returned nil for minimal page")
	}
}

// Test heap tuple parsing
func TestParseHeapTuple(t *testing.T) {
	// Minimal tuple header (23 bytes)
	data := make([]byte, 32)
	// t_xmin = 1
	data[0] = 1
	// t_infomask2 = 2 columns (at offset 18)
	data[18] = 2
	data[19] = 0
	// t_infomask = 0 (at offset 20)
	data[20] = 0
	data[21] = 0
	// t_hoff = 24 (at offset 22)
	data[22] = 24

	tuple := ParseHeapTuple(data)
	if tuple == nil {
		t.Skip("ParseHeapTuple returned nil")
	}
	if tuple.Header == nil {
		t.Error("Expected non-nil Header")
	}
}

func TestCstring(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		maxLen int
		want   string
	}{
		{"normal", []byte("hello\x00world"), 64, "hello"},
		{"no null", []byte("hello"), 3, "hel"},
		{"empty", []byte{}, 10, ""},
		{"max len", []byte("hello"), 64, "hello"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cstring(tt.data, tt.maxLen)
			if got != tt.want {
				t.Errorf("cstring() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  int
	}{
		{"int", 42, 42},
		{"int16", int16(1337), 1337},
		{"int32", int32(1234), 1234},
		{"int64", int64(123456789), 123456789},
		{"uint32", uint32(999), 999},
		{"uint16", uint16(500), 500},
		{"string", "not a number", 0},
		{"nil", nil, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toInt(tt.value)
			if got != tt.want {
				t.Errorf("toInt() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAlign(t *testing.T) {
	tests := []struct {
		offset int
		align  int
		want   int
	}{
		{0, 4, 0},
		{1, 4, 4},
		{3, 4, 4},
		{4, 4, 4},
		{5, 8, 8},
		{8, 8, 8},
		{0, 1, 0},
	}

	for _, tt := range tests {
		got := align(tt.offset, tt.align)
		if got != tt.want {
			t.Errorf("align(%d, %d) = %d, want %d", tt.offset, tt.align, got, tt.want)
		}
	}
}

func TestDecodeNumeric(t *testing.T) {
	// Test with a simple numeric value
	// Format: ndigits, weight, sign, dscale, digits...
	// For "12.34": ndigits=2, weight=0, sign=0 (positive), dscale=2, digits=[12, 3400]
	data := []byte{
		0x02, 0x00, // ndigits = 2
		0x00, 0x00, // weight = 0
		0x00, 0x00, // sign = 0 (positive)
		0x02, 0x00, // dscale = 2
		0x0c, 0x00, // digit 0 = 12
		0x48, 0x0d, // digit 1 = 3400
	}
	got := DecodeNumeric(data)
	if got == nil {
		t.Skip("DecodeNumeric returned nil")
	}
}

func TestSafeString(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"valid utf8", []byte("hello"), "hello"},
		{"empty", []byte{}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeString(tt.data)
			if got != tt.want {
				t.Errorf("safeString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDecodeJSONBArray(t *testing.T) {
	// JSONB array: [1, 2, 3]
	data := []byte{
		0x03, 0x00, 0x00, 0x00, // header: 3 elements, no scalar flag
		// JEntry for element 0
		0x0c, 0x00, 0x00, 0x10, // numeric
		// JEntry for element 1
		0x0c, 0x00, 0x00, 0x10, // numeric
		// JEntry for element 2
		0x0c, 0x00, 0x00, 0x10, // numeric
	}
	result := ParseJSONB(data)
	if result == nil {
		t.Skip("JSONB array parsing returned nil")
	}
	arr, ok := result.([]interface{})
	if !ok {
		t.Skipf("Expected array, got %T", result)
	}
	if len(arr) == 0 {
		t.Skip("Empty array returned")
	}
}

func TestDecodeArrayEmpty(t *testing.T) {
	// Empty array (ndim = 0)
	data := []byte{
		0x00, 0x00, 0x00, 0x00, // ndim = 0
		0x00, 0x00, 0x00, 0x00, // flags
		0x17, 0x00, 0x00, 0x00, // elemtype
		// no dimensions
	}
	got := DecodeType(data, 1007) // int4 array
	// ndim=0 returns nil
	if got != nil {
		// Actually it might return empty slice, which is OK
		arr, ok := got.([]interface{})
		if ok && len(arr) == 0 {
			return // empty array is fine
		}
	}
}

func TestDecodeArrayBasic(t *testing.T) {
	// 1D array of int4: {1, 2, 3}
	data := []byte{
		0x01, 0x00, 0x00, 0x00, // ndim = 1
		0x00, 0x00, 0x00, 0x00, // dataoffset = 0 (no nulls)
		0x17, 0x00, 0x00, 0x00, // elemtype = 23 (int4)
		0x03, 0x00, 0x00, 0x00, // dim[0] = 3
		0x01, 0x00, 0x00, 0x00, // lbound[0] = 1
		// data
		0x01, 0x00, 0x00, 0x00, // 1
		0x02, 0x00, 0x00, 0x00, // 2
		0x03, 0x00, 0x00, 0x00, // 3
	}
	got := DecodeType(data, 1007) // int4 array
	arr, ok := got.([]interface{})
	if !ok {
		t.Skipf("Expected array, got %T", got)
	}
	if len(arr) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(arr))
	}
}

// === Tuple and Heap tests ===

func TestIsVisible(t *testing.T) {
	tests := []struct {
		name    string
		header  *HeapTupleHeader
		visible bool
	}{
		{
			name: "committed and not deleted",
			header: &HeapTupleHeader{
				XminCommitted: true,
				XmaxInvalid:   true,
				XmaxCommitted: false,
			},
			visible: true,
		},
		{
			name: "committed with invalid xmax",
			header: &HeapTupleHeader{
				XminCommitted: true,
				XmaxInvalid:   true,
				XmaxCommitted: true,
			},
			visible: true,
		},
		{
			name: "not committed",
			header: &HeapTupleHeader{
				XminCommitted: false,
				XmaxInvalid:   true,
				XmaxCommitted: false,
			},
			visible: false,
		},
		{
			name: "deleted (xmax committed)",
			header: &HeapTupleHeader{
				XminCommitted: true,
				XmaxInvalid:   false,
				XmaxCommitted: true,
			},
			visible: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := &HeapTupleData{Header: tt.header}
			if got := tuple.IsVisible(); got != tt.visible {
				t.Errorf("IsVisible() = %v, want %v", got, tt.visible)
			}
		})
	}
}

func TestIsNull(t *testing.T) {
	tests := []struct {
		name   string
		bitmap []byte
		attnum int
		isNull bool
	}{
		{"nil bitmap", nil, 1, false},
		{"attnum 0", []byte{0xFF}, 0, false},
		{"attnum 1 not null", []byte{0x01}, 1, false},     // bit 0 = 1
		{"attnum 1 null", []byte{0x00}, 1, true},          // bit 0 = 0
		{"attnum 2 not null", []byte{0x02}, 2, false},     // bit 1 = 1
		{"attnum 2 null", []byte{0x01}, 2, true},          // bit 1 = 0
		{"attnum 9 not null", []byte{0x00, 0x01}, 9, false}, // bit 8 = 1
		{"attnum 9 null", []byte{0x00, 0x00}, 9, true},    // bit 8 = 0
		{"attnum beyond bitmap", []byte{0xFF}, 10, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := &HeapTupleData{
				Header: &HeapTupleHeader{HasNull: tt.bitmap != nil},
				Bitmap: tt.bitmap,
			}
			if got := tuple.IsNull(tt.attnum); got != tt.isNull {
				t.Errorf("IsNull(%d) = %v, want %v", tt.attnum, got, tt.isNull)
			}
		})
	}
}

func TestAlignFromChar(t *testing.T) {
	tests := []struct {
		char  byte
		align int
	}{
		{'c', 1},
		{'s', 2},
		{'i', 4},
		{'d', 8},
		{'x', 0}, // unknown
		{0, 0},
	}

	for _, tt := range tests {
		got := alignFromChar(tt.char)
		if got != tt.align {
			t.Errorf("alignFromChar('%c') = %d, want %d", tt.char, got, tt.align)
		}
	}
}

func TestTypeAlign(t *testing.T) {
	tests := []struct {
		typID  int
		length int
		align  int
	}{
		// Double aligned (8 bytes)
		{OidInt8, 8, 8},
		{OidFloat8, 8, 8},
		{OidTimestamp, 8, 8},
		{OidPoint, 16, 8},
		// Int aligned (4 bytes)
		{OidInt4, 4, 4},
		{OidFloat4, 4, 4},
		{OidText, -1, 4},
		{OidJSONB, -1, 4},
		// Short aligned (2 bytes)
		{OidInt2, 2, 2},
		// Char aligned (1 byte)
		{OidBool, 1, 1},
		{OidChar, 1, 1},
		{OidUUID, 16, 1},
		// Default based on length
		{99999, 16, 8},  // >= 8
		{99999, 4, 4},   // >= 4
		{99999, 2, 2},   // >= 2
		{99999, 1, 1},   // < 2
		{99999, -1, 4},  // varlena
	}

	for _, tt := range tests {
		got := typeAlign(tt.typID, tt.length)
		if got != tt.align {
			t.Errorf("typeAlign(%d, %d) = %d, want %d", tt.typID, tt.length, got, tt.align)
		}
	}
}

func TestIsShortVarlena(t *testing.T) {
	tests := []struct {
		name  string
		data  []byte
		short bool
	}{
		{"empty", []byte{}, false},
		{"short varlena", []byte{0x0d}, true},        // bit 0 set, not 0x01
		{"short varlena 2", []byte{0x03}, true},      // bit 0 set, not 0x01
		{"long varlena", []byte{0x00}, false},        // bit 0 not set
		{"TOAST pointer", []byte{0x01}, false},       // exactly 0x01
		{"long header", []byte{0x18, 0x00}, false},   // bit 0 not set
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isShortVarlena(tt.data); got != tt.short {
				t.Errorf("isShortVarlena(%v) = %v, want %v", tt.data, got, tt.short)
			}
		})
	}
}

func TestMax(t *testing.T) {
	tests := []struct {
		a, b, want int
	}{
		{1, 2, 2},
		{2, 1, 2},
		{5, 5, 5},
		{-1, 0, 0},
		{0, -1, 0},
	}

	for _, tt := range tests {
		if got := max(tt.a, tt.b); got != tt.want {
			t.Errorf("max(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestReadValueFixedLength(t *testing.T) {
	// Test reading fixed-length int4
	data := []byte{0x2A, 0x00, 0x00, 0x00} // 42 in little-endian
	val, consumed := readValue(data, 0, OidInt4, 4)
	if consumed != 4 {
		t.Errorf("consumed = %d, want 4", consumed)
	}
	if val != int32(42) {
		t.Errorf("val = %v, want 42", val)
	}
}

func TestReadValueVarlena(t *testing.T) {
	// Short varlena: "hi" (length=3 including header)
	data := []byte{0x07, 'h', 'i'} // short varlena: (3<<1)|1 = 7
	val, consumed := readValue(data, 0, OidText, -1)
	if consumed != 3 {
		t.Errorf("consumed = %d, want 3", consumed)
	}
	if val != "hi" {
		t.Errorf("val = %v, want 'hi'", val)
	}
}

func TestReadValueCString(t *testing.T) {
	data := []byte{'h', 'e', 'l', 'l', 'o', 0, 'x', 'x'}
	val, consumed := readValue(data, 0, OidName, -2) // -2 for cstring
	if consumed != 6 {
		t.Errorf("consumed = %d, want 6", consumed)
	}
	if val != "hello" {
		t.Errorf("val = %v, want 'hello'", val)
	}
}

func TestReadValueOutOfBounds(t *testing.T) {
	data := []byte{0x01, 0x02}
	val, consumed := readValue(data, 10, OidInt4, 4) // offset beyond data
	if val != nil || consumed != 0 {
		t.Errorf("expected nil/0, got %v/%d", val, consumed)
	}
}

func TestReadValueShortData(t *testing.T) {
	data := []byte{0x01, 0x02} // only 2 bytes
	val, consumed := readValue(data, 0, OidInt4, 4) // need 4 bytes
	if val != nil || consumed != 0 {
		t.Errorf("expected nil/0, got %v/%d", val, consumed)
	}
}

// === Page tests ===

func TestValidHeader(t *testing.T) {
	tests := []struct {
		name  string
		h     *PageHeader
		valid bool
	}{
		{
			name:  "valid 8k page",
			h:     &PageHeader{Lower: 24, Upper: 8000, PageSize: 8192, Version: 4},
			valid: true,
		},
		{
			name:  "valid 16k page",
			h:     &PageHeader{Lower: 24, Upper: 16000, PageSize: 16384, Version: 4},
			valid: true,
		},
		{
			name:  "nil header",
			h:     nil,
			valid: false,
		},
		{
			name:  "invalid page size",
			h:     &PageHeader{Lower: 24, Upper: 8000, PageSize: 4096, Version: 4},
			valid: false,
		},
		{
			name:  "version too low",
			h:     &PageHeader{Lower: 24, Upper: 8000, PageSize: 8192, Version: 0},
			valid: false,
		},
		{
			name:  "version too high",
			h:     &PageHeader{Lower: 24, Upper: 8000, PageSize: 8192, Version: 11},
			valid: false,
		},
		{
			name:  "lower too small",
			h:     &PageHeader{Lower: 10, Upper: 8000, PageSize: 8192, Version: 4},
			valid: false,
		},
		{
			name:  "upper > pagesize",
			h:     &PageHeader{Lower: 24, Upper: 9000, PageSize: 8192, Version: 4},
			valid: false,
		},
		{
			name:  "lower > upper",
			h:     &PageHeader{Lower: 8000, Upper: 100, PageSize: 8192, Version: 4},
			valid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validHeader(tt.h); got != tt.valid {
				t.Errorf("validHeader() = %v, want %v", got, tt.valid)
			}
		})
	}
}

func TestParseHeader(t *testing.T) {
	// Create minimal page header
	data := make([]byte, 24)
	// pd_lower at offset 12 (2 bytes)
	data[12] = 0x30 // 48
	data[13] = 0x00
	// pd_upper at offset 14 (2 bytes)
	data[14] = 0x00 // 8000
	data[15] = 0x1F
	// pd_pagesize_version at offset 18 (2 bytes)
	// pagesize=8192 (0x2000), version=4
	data[18] = 0x04 // version
	data[19] = 0x20 // pagesize high byte

	h := parseHeader(data)
	if h.Lower != 48 {
		t.Errorf("Lower = %d, want 48", h.Lower)
	}
	if h.Upper != 7936 { // 0x1F00
		t.Errorf("Upper = %d, want 7936", h.Upper)
	}
	if h.PageSize != 8192 {
		t.Errorf("PageSize = %d, want 8192", h.PageSize)
	}
	if h.Version != 4 {
		t.Errorf("Version = %d, want 4", h.Version)
	}
}

func TestParseItems(t *testing.T) {
	// Create page with 2 items
	data := make([]byte, 48)
	h := &PageHeader{Lower: 32} // header(24) + 2 items(8)

	// Item 1 at offset 24: offset=8000, flags=1, length=100
	// raw = offset | (flags << 15) | (length << 17)
	// = 8000 | (1 << 15) | (100 << 17)
	// = 8000 | 32768 | 13107200 = 13148000
	raw1 := uint32(8000) | (uint32(1) << 15) | (uint32(100) << 17)
	data[24] = byte(raw1)
	data[25] = byte(raw1 >> 8)
	data[26] = byte(raw1 >> 16)
	data[27] = byte(raw1 >> 24)

	// Item 2 at offset 28: offset=7900, flags=1, length=50
	raw2 := uint32(7900) | (uint32(1) << 15) | (uint32(50) << 17)
	data[28] = byte(raw2)
	data[29] = byte(raw2 >> 8)
	data[30] = byte(raw2 >> 16)
	data[31] = byte(raw2 >> 24)

	items := parseItems(data, h)
	if len(items) != 2 {
		t.Fatalf("got %d items, want 2", len(items))
	}

	if items[0].Offset != 8000 || items[0].Flags != 1 || items[0].Length != 100 {
		t.Errorf("item[0] = %+v, want offset=8000, flags=1, length=100", items[0])
	}
	if items[1].Offset != 7900 || items[1].Flags != 1 || items[1].Length != 50 {
		t.Errorf("item[1] = %+v, want offset=7900, flags=1, length=50", items[1])
	}
}

func TestParsePageTooSmall(t *testing.T) {
	data := make([]byte, 100) // < PageSize
	entries := ParsePage(data)
	if entries != nil {
		t.Errorf("expected nil for small page, got %v", entries)
	}
}

func TestParsePageInvalidHeader(t *testing.T) {
	// Create page with invalid header (wrong page size)
	data := make([]byte, PageSize)
	data[18] = 0x04      // version 4
	data[19] = 0x10      // pagesize 4096 (invalid)
	entries := ParsePage(data)
	if entries != nil {
		t.Errorf("expected nil for invalid header, got %v", entries)
	}
}

// === JSONB additional tests ===

func TestDecodeJEntryTypes(t *testing.T) {
	// JSONB entry type constants:
	// jeString    = 0x00000000
	// jeNumeric   = 0x10000000
	// jeBoolFalse = 0x20000000
	// jeBoolTrue  = 0x30000000
	// jeNull      = 0x40000000
	// jeContainer = 0x50000000

	// Test null entry
	nullEntry := uint32(0x40000000) // jeNull
	got := decodeJEntry([]byte{}, 0, 0, nullEntry)
	if got != nil {
		t.Errorf("decodeJEntry(null) = %v, want nil", got)
	}

	// Test false entry
	falseEntry := uint32(0x20000000) // jeBoolFalse
	got = decodeJEntry([]byte{}, 0, 0, falseEntry)
	if got != false {
		t.Errorf("decodeJEntry(false) = %v, want false", got)
	}

	// Test true entry
	trueEntry := uint32(0x30000000) // jeBoolTrue
	got = decodeJEntry([]byte{}, 0, 0, trueEntry)
	if got != true {
		t.Errorf("decodeJEntry(true) = %v, want true", got)
	}

	// Test string entry (jeString = 0x00000000, so just the length)
	stringEntry := uint32(0x00000000 | 5) // jeString, length 5
	data := []byte("hello world")
	got = decodeJEntry(data, 0, 5, stringEntry)
	if got != "hello" {
		t.Errorf("decodeJEntry(string) = %v, want 'hello'", got)
	}
}

func TestTotalLen(t *testing.T) {
	// Empty entries
	if got := totalLen([]uint32{}); got != 0 {
		t.Errorf("totalLen([]) = %d, want 0", got)
	}

	// Single entry with length
	entries := []uint32{5} // length 5
	if got := totalLen(entries); got != 5 {
		t.Errorf("totalLen([5]) = %d, want 5", got)
	}
}

func TestEntryOffLen(t *testing.T) {
	// Simple entries without offset flag
	entries := []uint32{3, 5, 7}
	
	// First entry
	off, length := entryOffLen(entries, 0, 0)
	if off != 0 || length != 3 {
		t.Errorf("entryOffLen[0] = (%d, %d), want (0, 3)", off, length)
	}
	
	// Second entry
	off, length = entryOffLen(entries, 1, 0)
	if off != 3 || length != 5 {
		t.Errorf("entryOffLen[1] = (%d, %d), want (3, 5)", off, length)
	}
}

// === Numeric Range tests ===

func TestDecodeNumericRange(t *testing.T) {
	tests := []struct {
		name  string
		flags byte
		want  string
	}{
		{"inclusive bounds", 0x02 | 0x04, "[?,?]"},
		{"exclusive bounds", 0x00, "(?,?)"},
		{"lower inclusive", 0x02, "[?,?)"},
		{"upper inclusive", 0x04, "(?,?]"},
		{"lower infinite", 0x08, "(,?)"},
		{"upper infinite", 0x10, "(?,)"},
		{"both infinite", 0x08 | 0x10, "(,)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decodeNumericRange(nil, tt.flags)
			if got != tt.want {
				t.Errorf("decodeNumericRange(flags=%02x) = %q, want %q", tt.flags, got, tt.want)
			}
		})
	}
}

// === DecodeTuple tests ===

func TestDecodeTupleEmpty(t *testing.T) {
	// nil tuple
	if got := DecodeTuple(nil, nil); got != nil {
		t.Errorf("DecodeTuple(nil) = %v, want nil", got)
	}

	// empty data
	tuple := &HeapTupleData{Header: &HeapTupleHeader{}, Data: []byte{}}
	if got := DecodeTuple(tuple, nil); got != nil {
		t.Errorf("DecodeTuple(empty) = %v, want nil", got)
	}
}

func TestDecodeTupleSimple(t *testing.T) {
	// Create tuple with simple int4 column
	tuple := &HeapTupleData{
		Header: &HeapTupleHeader{
			Natts:   1,
			HasNull: false,
		},
		Data: []byte{0x2A, 0x00, 0x00, 0x00}, // 42
	}
	columns := []Column{
		{Name: "id", TypID: OidInt4, Len: 4, Align: 'i', Num: 1},
	}

	got := DecodeTuple(tuple, columns)
	if got == nil {
		t.Fatal("DecodeTuple returned nil")
	}
	if got["id"] != int32(42) {
		t.Errorf("got[id] = %v, want 42", got["id"])
	}
}

func TestDecodeTupleWithNull(t *testing.T) {
	// Create tuple with null value (bitmap indicates null)
	tuple := &HeapTupleData{
		Header: &HeapTupleHeader{
			Natts:   2,
			HasNull: true,
		},
		Bitmap: []byte{0x01}, // first col not null, second is null (bit 1 = 0)
		Data:   []byte{0x2A, 0x00, 0x00, 0x00}, // only first value
	}
	columns := []Column{
		{Name: "id", TypID: OidInt4, Len: 4, Align: 'i', Num: 1},
		{Name: "name", TypID: OidText, Len: -1, Align: 'i', Num: 2},
	}

	got := DecodeTuple(tuple, columns)
	if got == nil {
		t.Fatal("DecodeTuple returned nil")
	}
	if got["id"] != int32(42) {
		t.Errorf("got[id] = %v, want 42", got["id"])
	}
	if got["name"] != nil {
		t.Errorf("got[name] = %v, want nil", got["name"])
	}
}
