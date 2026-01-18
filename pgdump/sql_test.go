package pgdump

import (
	"bytes"
	"strings"
	"testing"
)

func TestTableToSQL(t *testing.T) {
	table := TableDump{
		Name:     "users",
		OID:      16384,
		Filenode: 16384,
		Columns: []ColumnInfo{
			{Name: "id", Type: "int4", TypID: OidInt4},
			{Name: "name", Type: "text", TypID: OidText},
			{Name: "active", Type: "bool", TypID: OidBool},
		},
		Rows: []map[string]interface{}{
			{"id": int32(1), "name": "alice", "active": true},
			{"id": int32(2), "name": "bob", "active": false},
		},
		RowCount: 2,
	}

	var buf bytes.Buffer
	err := table.ToSQL(&buf)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	sql := buf.String()

	// Check CREATE TABLE
	if !strings.Contains(sql, "CREATE TABLE IF NOT EXISTS users") {
		t.Error("Missing CREATE TABLE statement")
	}
	if !strings.Contains(sql, "id INTEGER") {
		t.Error("Missing id column definition")
	}
	if !strings.Contains(sql, "name TEXT") {
		t.Error("Missing name column definition")
	}
	if !strings.Contains(sql, "active BOOLEAN") {
		t.Error("Missing active column definition")
	}

	// Check INSERT
	if !strings.Contains(sql, "INSERT INTO users") {
		t.Error("Missing INSERT statement")
	}
	if !strings.Contains(sql, "'alice'") {
		t.Error("Missing alice value")
	}
	if !strings.Contains(sql, "'bob'") {
		t.Error("Missing bob value")
	}
	if !strings.Contains(sql, "TRUE") {
		t.Error("Missing TRUE value")
	}
	if !strings.Contains(sql, "FALSE") {
		t.Error("Missing FALSE value")
	}
}

func TestDatabaseToSQL(t *testing.T) {
	db := DatabaseDump{
		OID:  16384,
		Name: "testdb",
		Tables: []TableDump{
			{
				Name: "simple",
				Columns: []ColumnInfo{
					{Name: "val", Type: "int4", TypID: OidInt4},
				},
				Rows: []map[string]interface{}{
					{"val": int32(42)},
				},
				RowCount: 1,
			},
		},
	}

	var buf bytes.Buffer
	err := db.ToSQL(&buf)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	sql := buf.String()
	if !strings.Contains(sql, "CREATE TABLE") {
		t.Error("Missing CREATE TABLE")
	}
	if !strings.Contains(sql, "42") {
		t.Error("Missing value 42")
	}
}

func TestDumpResultToSQL(t *testing.T) {
	result := DumpResult{
		Databases: []DatabaseDump{
			{
				OID:  16384,
				Name: "mydb",
				Tables: []TableDump{
					{
						Name: "test",
						Columns: []ColumnInfo{
							{Name: "x", Type: "int4", TypID: OidInt4},
						},
						Rows:     []map[string]interface{}{{"x": int32(1)}},
						RowCount: 1,
					},
				},
			},
		},
	}

	var buf bytes.Buffer
	err := result.ToSQL(&buf)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	sql := buf.String()
	if !strings.Contains(sql, "-- Database: mydb") {
		t.Error("Missing database comment")
	}
	if !strings.Contains(sql, "\\connect mydb") {
		t.Error("Missing connect command")
	}
}

func TestFormatSQLValueTypes(t *testing.T) {
	tests := []struct {
		name  string
		val   interface{}
		typID int
		want  string
	}{
		{"nil", nil, 0, "NULL"},
		{"bool true", true, OidBool, "TRUE"},
		{"bool false", false, OidBool, "FALSE"},
		{"int32", int32(42), OidInt4, "42"},
		{"int64", int64(123456789), OidInt8, "123456789"},
		{"float64", float64(3.14), OidFloat8, "3.14"},
		{"string", "hello", OidText, "'hello'"},
		{"string with quote", "it's", OidText, "'it''s'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatSQLValue(tt.val, tt.typID)
			if got != tt.want {
				t.Errorf("formatSQLValue(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}

func TestFormatSQLValueArray(t *testing.T) {
	arr := []interface{}{int32(1), int32(2), int32(3)}
	got := formatSQLValue(arr, 0)
	if got != "ARRAY[1, 2, 3]" {
		t.Errorf("formatSQLValue(array) = %q, want ARRAY[1, 2, 3]", got)
	}
}

func TestFormatSQLValueJSON(t *testing.T) {
	m := map[string]interface{}{"key": "value"}
	got := formatSQLValue(m, OidJSONB)
	if !strings.Contains(got, "key") || !strings.Contains(got, "value") {
		t.Errorf("formatSQLValue(json) = %q, expected JSON with key/value", got)
	}
}

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "simple"},
		{"with space", "\"with space\""},
		{"select", "\"select\""},  // reserved word
		{"has\"quote", "\"has\"\"quote\""},
	}

	for _, tt := range tests {
		got := quoteIdent(tt.input)
		if got != tt.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestQuoteLiteral(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"simple", "'simple'"},
		{"with'quote", "'with''quote'"},
	}

	for _, tt := range tests {
		got := quoteLiteral(tt.input)
		if got != tt.want {
			t.Errorf("quoteLiteral(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestPgTypeToSQL(t *testing.T) {
	tests := []struct {
		typID int
		want  string
	}{
		{OidBool, "BOOLEAN"},
		{OidInt4, "INTEGER"},
		{OidInt8, "BIGINT"},
		{OidText, "TEXT"},
		{OidJSONB, "JSONB"},
		{OidTimestamp, "TIMESTAMP"},
		{OidUUID, "UUID"},
		{OidInet, "INET"},
		{0, "TEXT"}, // unknown
	}

	for _, tt := range tests {
		got := pgTypeToSQL("", tt.typID)
		if got != tt.want {
			t.Errorf("pgTypeToSQL(%d) = %q, want %q", tt.typID, got, tt.want)
		}
	}
}

func TestEmptyTable(t *testing.T) {
	table := TableDump{
		Name: "empty",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int4", TypID: OidInt4},
		},
		Rows:     nil,
		RowCount: 0,
	}

	var buf bytes.Buffer
	err := table.ToSQL(&buf)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	sql := buf.String()
	if !strings.Contains(sql, "CREATE TABLE") {
		t.Error("Missing CREATE TABLE for empty table")
	}
	if strings.Contains(sql, "INSERT") {
		t.Error("Should not have INSERT for empty table")
	}
}

func TestNullValues(t *testing.T) {
	table := TableDump{
		Name: "nulltest",
		Columns: []ColumnInfo{
			{Name: "id", Type: "int4", TypID: OidInt4},
			{Name: "name", Type: "text", TypID: OidText},
		},
		Rows: []map[string]interface{}{
			{"id": int32(1), "name": nil},
		},
		RowCount: 1,
	}

	var buf bytes.Buffer
	err := table.ToSQL(&buf)
	if err != nil {
		t.Fatalf("ToSQL failed: %v", err)
	}

	sql := buf.String()
	if !strings.Contains(sql, "NULL") {
		t.Error("Missing NULL value")
	}
}
