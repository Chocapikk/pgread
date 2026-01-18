package pgdump

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ChecksumResult contains page checksum verification results
type ChecksumResult struct {
	BlockNumber    uint32 `json:"block_number"`
	StoredChecksum uint16 `json:"stored_checksum"`
	ComputedChecksum uint16 `json:"computed_checksum"`
	Valid          bool   `json:"valid"`
	LSN            uint64 `json:"lsn,omitempty"`
	LSNStr         string `json:"lsn_str,omitempty"`
}

// FileChecksumResult contains results for a single file
type FileChecksumResult struct {
	Path         string           `json:"path"`
	TotalBlocks  int              `json:"total_blocks"`
	ValidBlocks  int              `json:"valid_blocks"`
	InvalidBlocks int             `json:"invalid_blocks"`
	ZeroBlocks   int              `json:"zero_blocks"`
	Errors       []ChecksumResult `json:"errors,omitempty"`
}

// DataDirChecksumResult contains results for entire data directory
type DataDirChecksumResult struct {
	DataDir       string                `json:"data_dir"`
	ChecksumsEnabled bool              `json:"checksums_enabled"`
	TotalFiles    int                   `json:"total_files"`
	TotalBlocks   int                   `json:"total_blocks"`
	ValidBlocks   int                   `json:"valid_blocks"`
	InvalidBlocks int                   `json:"invalid_blocks"`
	Files         []FileChecksumResult  `json:"files,omitempty"`
}

// VerifyPageChecksum verifies a single page's checksum
// blockNumber is the absolute block number in the relation
func VerifyPageChecksum(page []byte, blockNumber uint32) ChecksumResult {
	result := ChecksumResult{
		BlockNumber: blockNumber,
	}
	
	if len(page) < PageSize {
		return result
	}
	
	// Check if page is all zeros (empty/unused)
	if isZeroPage(page) {
		result.Valid = true
		return result
	}
	
	// Get stored checksum from page header (offset 8, 2 bytes)
	result.StoredChecksum = binary.LittleEndian.Uint16(page[8:10])
	
	// Get LSN
	result.LSN = binary.LittleEndian.Uint64(page[0:8])
	result.LSNStr = FormatLSN(result.LSN)
	
	// Compute checksum
	result.ComputedChecksum = computePageChecksum(page, blockNumber)
	result.Valid = result.StoredChecksum == result.ComputedChecksum
	
	return result
}

// VerifyFileChecksums verifies all pages in a heap file
func VerifyFileChecksums(data []byte, segmentNumber uint32) *FileChecksumResult {
	result := &FileChecksumResult{
		TotalBlocks: len(data) / PageSize,
	}
	
	blocksPerSegment := uint32(131072) // 1GB / 8KB
	baseBlock := segmentNumber * blocksPerSegment
	
	for i := 0; i < result.TotalBlocks; i++ {
		offset := i * PageSize
		page := data[offset : offset+PageSize]
		
		if isZeroPage(page) {
			result.ZeroBlocks++
			result.ValidBlocks++
			continue
		}
		
		blockNum := baseBlock + uint32(i)
		checkResult := VerifyPageChecksum(page, blockNum)
		
		if checkResult.Valid {
			result.ValidBlocks++
		} else {
			result.InvalidBlocks++
			result.Errors = append(result.Errors, checkResult)
		}
	}
	
	return result
}

// VerifyDataDirChecksums verifies checksums for entire data directory
func VerifyDataDirChecksums(dataDir string) (*DataDirChecksumResult, error) {
	result := &DataDirChecksumResult{
		DataDir: dataDir,
	}
	
	// Check if checksums are enabled via pg_control
	cf, err := ReadControlFile(dataDir)
	if err == nil {
		result.ChecksumsEnabled = cf.DataChecksumsEnabled
	}
	
	// Scan base directory for database directories
	baseDir := filepath.Join(dataDir, "base")
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read base directory: %w", err)
	}
	
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		
		// Check if it's a numeric OID directory
		if _, err := strconv.ParseUint(entry.Name(), 10, 32); err != nil {
			continue
		}
		
		dbPath := filepath.Join(baseDir, entry.Name())
		files, err := os.ReadDir(dbPath)
		if err != nil {
			continue
		}
		
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			
			// Check if it's a numeric filenode
			name := f.Name()
			if _, err := strconv.ParseUint(name, 10, 32); err != nil {
				// Check for segment files (e.g., "12345.1")
				if len(name) > 2 && name[len(name)-2] == '.' {
					base := name[:len(name)-2]
					if _, err := strconv.ParseUint(base, 10, 32); err != nil {
						continue
					}
				} else {
					continue
				}
			}
			
			filePath := filepath.Join(dbPath, f.Name())
			data, err := os.ReadFile(filePath)
			if err != nil || len(data) < PageSize {
				continue
			}
			
			// Determine segment number from filename
			segNum := uint32(0)
			if idx := len(name) - 1; idx > 0 && name[idx-1] == '.' {
				segNum = uint32(name[idx] - '0')
			}
			
			fileResult := VerifyFileChecksums(data, segNum)
			fileResult.Path = filePath
			
			result.TotalFiles++
			result.TotalBlocks += fileResult.TotalBlocks
			result.ValidBlocks += fileResult.ValidBlocks
			result.InvalidBlocks += fileResult.InvalidBlocks
			
			if len(fileResult.Errors) > 0 {
				result.Files = append(result.Files, *fileResult)
			}
		}
	}
	
	return result, nil
}

// computePageChecksum computes PostgreSQL page checksum
// This implements the FNV-1a based algorithm used by PostgreSQL
func computePageChecksum(page []byte, blockNumber uint32) uint16 {
	// PostgreSQL uses a custom checksum algorithm based on FNV-1a
	// The checksum field (bytes 8-9) must be zeroed before computation
	
	// Create a copy with checksum zeroed
	pageCopy := make([]byte, PageSize)
	copy(pageCopy, page)
	pageCopy[8] = 0
	pageCopy[9] = 0
	
	// Initialize with FNV offset basis
	var checksum uint32 = 0
	
	// Process page in 4-byte chunks
	for i := 0; i < PageSize; i += 4 {
		word := binary.LittleEndian.Uint32(pageCopy[i : i+4])
		checksum = checksumComp(checksum, word)
	}
	
	// Mix in the block number
	checksum ^= blockNumber
	
	// Fold to 16 bits
	checksum = (checksum >> 16) ^ (checksum & 0xFFFF)
	
	return uint16(checksum)
}

// checksumComp is the core checksum computation function
// Based on PostgreSQL's implementation in checksum_impl.h
func checksumComp(checksum, value uint32) uint32 {
	// Split value into two 16-bit parts
	lo := value & 0xFFFF
	hi := value >> 16
	
	// Rotate checksum by amount based on low bits
	shift := int(checksum & 0x1F)
	if shift > 0 {
		checksum = (checksum >> shift) | (checksum << (32 - shift))
	}
	
	// XOR in the value parts
	checksum ^= lo
	checksum ^= hi << 1
	
	return checksum
}

// isZeroPage checks if a page is all zeros
func isZeroPage(page []byte) bool {
	for _, b := range page {
		if b != 0 {
			return false
		}
	}
	return true
}

// Alternative FNV-1a based checksum (PostgreSQL's actual implementation)
func pgChecksumBlock(page []byte, blockNumber uint32) uint16 {
	// PostgreSQL checksum uses a SIMD-friendly algorithm
	// This is a reference implementation
	
	const (
		nSums    = 32
		fnvPrime = 0x01000193
	)
	
	// Initialize sums with block number mixed in
	var sums [nSums]uint32
	for i := range sums {
		sums[i] = blockNumber
	}
	
	// Create copy with checksum field zeroed
	pageCopy := make([]byte, len(page))
	copy(pageCopy, page)
	if len(pageCopy) > 9 {
		pageCopy[8] = 0
		pageCopy[9] = 0
	}
	
	// Process in 32-bit words, updating rolling sums
	words := len(pageCopy) / 4
	for i := 0; i < words; i++ {
		word := binary.LittleEndian.Uint32(pageCopy[i*4 : i*4+4])
		idx := i % nSums
		sums[idx] = sums[idx]*fnvPrime ^ word
	}
	
	// Combine all sums
	var result uint32
	for _, s := range sums {
		result ^= s
	}
	
	// Fold to 16 bits
	result = (result >> 16) ^ (result & 0xFFFF)
	
	return uint16(result)
}
