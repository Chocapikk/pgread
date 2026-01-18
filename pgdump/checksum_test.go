package pgdump

import (
	"testing"
)

func TestVerifyPageChecksum(t *testing.T) {
	// Create a test page
	page := make([]byte, PageSize)
	
	// Set up basic page header
	// LSN (0-7)
	putU64(page, 0, 0x0000000100000001)
	// Checksum will be at offset 8-9
	// Lower (12-13)
	putU16(page, 12, 28)
	// Upper (14-15)
	putU16(page, 14, 8000)
	// Special (16-17)
	putU16(page, 16, 8192)
	// Page size + version (18-19)
	putU16(page, 18, 8192|4)
	
	// Compute and store checksum
	checksum := computePageChecksum(page, 0)
	putU16(page, 8, checksum)
	
	// Verify
	result := VerifyPageChecksum(page, 0)
	
	if !result.Valid {
		t.Errorf("VerifyPageChecksum: expected valid, got invalid. Stored=%d, Computed=%d",
			result.StoredChecksum, result.ComputedChecksum)
	}
	
	if result.BlockNumber != 0 {
		t.Errorf("BlockNumber = %d, want 0", result.BlockNumber)
	}
}

func TestVerifyPageChecksumInvalid(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set up page header
	putU64(page, 0, 0x0000000100000001)
	putU16(page, 8, 0x1234) // Wrong checksum
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	putU16(page, 16, 8192)
	putU16(page, 18, 8192|4)
	
	// Add some data to make it non-zero
	page[100] = 0xFF
	
	result := VerifyPageChecksum(page, 0)
	
	if result.Valid {
		t.Error("VerifyPageChecksum: expected invalid for wrong checksum")
	}
}

func TestVerifyPageChecksumZeroPage(t *testing.T) {
	page := make([]byte, PageSize)
	// All zeros
	
	result := VerifyPageChecksum(page, 0)
	
	if !result.Valid {
		t.Error("VerifyPageChecksum: zero page should be valid")
	}
}

func TestIsZeroPage(t *testing.T) {
	// Zero page
	zeroPage := make([]byte, PageSize)
	if !isZeroPage(zeroPage) {
		t.Error("isZeroPage returned false for zero page")
	}
	
	// Non-zero page
	nonZeroPage := make([]byte, PageSize)
	nonZeroPage[100] = 1
	if isZeroPage(nonZeroPage) {
		t.Error("isZeroPage returned true for non-zero page")
	}
}

func TestVerifyFileChecksums(t *testing.T) {
	// Create data with multiple pages
	numPages := 3
	data := make([]byte, PageSize*numPages)
	
	for i := 0; i < numPages; i++ {
		offset := i * PageSize
		page := data[offset : offset+PageSize]
		
		// Set up page header
		putU64(page, 0, uint64(i+1))
		putU16(page, 12, 28)
		putU16(page, 14, 8000)
		putU16(page, 16, 8192)
		putU16(page, 18, 8192|4)
		
		// Compute and store checksum
		checksum := computePageChecksum(page, uint32(i))
		putU16(page, 8, checksum)
	}
	
	result := VerifyFileChecksums(data, 0)
	
	if result.TotalBlocks != numPages {
		t.Errorf("TotalBlocks = %d, want %d", result.TotalBlocks, numPages)
	}
	
	if result.ValidBlocks != numPages {
		t.Errorf("ValidBlocks = %d, want %d", result.ValidBlocks, numPages)
	}
	
	if result.InvalidBlocks != 0 {
		t.Errorf("InvalidBlocks = %d, want 0", result.InvalidBlocks)
	}
}

func TestComputePageChecksum(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set some data
	putU64(page, 0, 0x0000000100000001)
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	page[100] = 0xAB
	page[200] = 0xCD
	
	// Compute checksum for same page twice should be same
	cs1 := computePageChecksum(page, 0)
	cs2 := computePageChecksum(page, 0)
	
	if cs1 != cs2 {
		t.Errorf("Checksum not deterministic: %d != %d", cs1, cs2)
	}
	
	// Different block number should give different checksum
	cs3 := computePageChecksum(page, 1)
	if cs1 == cs3 {
		t.Error("Checksum should differ for different block numbers")
	}
	
	// Different data should give different checksum
	page[100] = 0xFF
	cs4 := computePageChecksum(page, 0)
	if cs1 == cs4 {
		t.Error("Checksum should differ for different data")
	}
}

func TestChecksumComp(t *testing.T) {
	// Test that checksumComp produces consistent results
	result1 := checksumComp(0, 0x12345678)
	result2 := checksumComp(0, 0x12345678)
	
	if result1 != result2 {
		t.Errorf("checksumComp not deterministic: %d != %d", result1, result2)
	}
	
	// Different input should give different output
	result3 := checksumComp(0, 0x87654321)
	if result1 == result3 {
		t.Error("checksumComp should differ for different input")
	}
}

func TestPgChecksumBlock(t *testing.T) {
	page := make([]byte, PageSize)
	
	// Set some data
	putU64(page, 0, 0x0000000100000001)
	putU16(page, 12, 28)
	putU16(page, 14, 8000)
	
	cs := pgChecksumBlock(page, 0)
	
	// Just verify it returns something and is deterministic
	cs2 := pgChecksumBlock(page, 0)
	if cs != cs2 {
		t.Errorf("pgChecksumBlock not deterministic: %d != %d", cs, cs2)
	}
}

// Helper
func putU16(data []byte, offset int, val uint16) {
	data[offset] = byte(val)
	data[offset+1] = byte(val >> 8)
}
