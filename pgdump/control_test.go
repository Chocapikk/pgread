package pgdump

import (
	"testing"
	"time"
)

func TestParseControlFile(t *testing.T) {
	// Create a minimal control file structure
	data := make([]byte, 300)
	
	// System identifier (offset 0)
	putU64(data, 0, 7123456789012345678)
	
	// Control version (offset 8)
	putU32(data, 8, 1300) // PG 15/16
	
	// Catalog version (offset 12)
	putU32(data, 12, 202307071) // PG 16
	
	// State (offset 16) - IN_PRODUCTION
	putU32(data, 16, uint32(DBStateInProduction))
	
	// Time (offset 20) - some timestamp
	putU64(data, 20, 800000000000000) // microseconds since 2000-01-01
	
	// Checkpoint LSN (offset 28)
	putU64(data, 28, 0x0000000100000050)
	
	cf, err := ParseControlFile(data)
	if err != nil {
		t.Fatalf("ParseControlFile failed: %v", err)
	}
	
	if cf.SystemIdentifier != 7123456789012345678 {
		t.Errorf("SystemIdentifier = %d, want 7123456789012345678", cf.SystemIdentifier)
	}
	
	if cf.PGControlVersion != 1300 {
		t.Errorf("PGControlVersion = %d, want 1300", cf.PGControlVersion)
	}
	
	if cf.State != DBStateInProduction {
		t.Errorf("State = %v, want in production", cf.State)
	}
	
	if cf.StateString != "in production" {
		t.Errorf("StateString = %q, want 'in production'", cf.StateString)
	}
}

func TestDBStateString(t *testing.T) {
	tests := []struct {
		state DBState
		want  string
	}{
		{DBStateStartup, "starting up"},
		{DBStateShutdowned, "shut down"},
		{DBStateShutdownedInRecovery, "shut down in recovery"},
		{DBStateShutdowning, "shutting down"},
		{DBStateInCrashRecovery, "in crash recovery"},
		{DBStateInArchiveRecovery, "in archive recovery"},
		{DBStateInProduction, "in production"},
		{DBState(99), "unknown (99)"},
	}
	
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.want {
			t.Errorf("DBState(%d).String() = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestPgEpochToTime(t *testing.T) {
	// pgEpochToTime uses Unix epoch (1970-01-01)
	unixEpoch := pgEpochToTime(0)
	want := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	
	if !unixEpoch.Equal(want) {
		t.Errorf("pgEpochToTime(0) = %v, want %v", unixEpoch, want)
	}
	
	// Test known timestamp: 2026-01-18 10:46:23 UTC = 1768733183
	knownTime := pgEpochToTime(1768733183)
	wantTime := time.Date(2026, 1, 18, 10, 46, 23, 0, time.UTC)
	
	if !knownTime.Equal(wantTime) {
		t.Errorf("pgEpochToTime(1768733183) = %v, want %v", knownTime, wantTime)
	}
}

func TestInferPGVersion(t *testing.T) {
	tests := []struct {
		controlVersion uint32
		catalogVersion uint32
		want           int
	}{
		{1300, 202307071, 16},
		{1300, 202209061, 15},
		{1201, 202107181, 14},
		{1201, 202007201, 13},
		{1100, 201909212, 12},
		{1100, 201806231, 11},
		{1002, 201707211, 10},
		{960, 201608131, 9},
	}
	
	for _, tt := range tests {
		got := inferPGVersion(tt.controlVersion, tt.catalogVersion)
		if got != tt.want {
			t.Errorf("inferPGVersion(%d, %d) = %d, want %d",
				tt.controlVersion, tt.catalogVersion, got, tt.want)
		}
	}
}

func TestMakeCRC32CTable(t *testing.T) {
	table := makeCRC32CTable()
	
	// Verify table has 256 entries
	if len(table) != 256 {
		t.Errorf("CRC table has %d entries, want 256", len(table))
	}
	
	// Verify some known values for CRC-32C
	// Entry 0 should be 0
	if table[0] != 0 {
		t.Errorf("table[0] = %d, want 0", table[0])
	}
	
	// Entry 1 should be the polynomial (for CRC-32C reflected)
	if table[1] != 0xF26B8303 {
		t.Errorf("table[1] = 0x%08X, want 0xF26B8303", table[1])
	}
}

func TestVerifyCRC32C(t *testing.T) {
	// Test with known data and CRC
	data := []byte("test")
	
	// Compute CRC
	crc := uint32(0xFFFFFFFF)
	table := makeCRC32CTable()
	for _, b := range data {
		crc = table[(crc^uint32(b))&0xFF] ^ (crc >> 8)
	}
	crc ^= 0xFFFFFFFF
	
	// Verify
	if !verifyCRC32C(data, crc) {
		t.Error("verifyCRC32C returned false for correct CRC")
	}
	
	// Test with wrong CRC
	if verifyCRC32C(data, crc+1) {
		t.Error("verifyCRC32C returned true for incorrect CRC")
	}
}

func TestControlFileTooSmall(t *testing.T) {
	data := make([]byte, 100)
	_, err := ParseControlFile(data)
	if err == nil {
		t.Error("expected error for small control file")
	}
}

// Helper functions
func putU32(data []byte, offset int, val uint32) {
	data[offset] = byte(val)
	data[offset+1] = byte(val >> 8)
	data[offset+2] = byte(val >> 16)
	data[offset+3] = byte(val >> 24)
}

func putU64(data []byte, offset int, val uint64) {
	for i := 0; i < 8; i++ {
		data[offset+i] = byte(val >> (i * 8))
	}
}
