package pgdump

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/pierrec/lz4/v4"
)

// TOASTPointer represents a TOAST pointer in PostgreSQL
// Format: varatt_external (18 bytes):
//
//	va_rawsize (4), va_extsize (4), va_valueid (4), va_toastrelid (4)
type TOASTPointer struct {
	RawSize      uint32 // Original uncompressed size
	ExtSize      uint32 // External (compressed) size
	ValueID      uint32 // chunk_id in TOAST table
	ToastRelID   uint32 // OID of TOAST table
	IsCompressed bool
	CompressionMethod int // 0=pglz, 1=lz4
}

// TOASTChunk represents a chunk from a TOAST table
type TOASTChunk struct {
	ChunkID  uint32
	ChunkSeq int32
	Data     []byte
}

// TOAST compression methods
const (
	ToastCompressionPGLZ = 0
	ToastCompressionLZ4  = 1
)

// TOAST varlena tags
const (
	VarTagExternal           = 0x01
	VarTagCompressedExternal = 0x02
	VarTagIndirect           = 0x01 // With high bit of size
)

// ParseTOASTPointer extracts TOAST pointer info from a varlena value.
// Expected layout: [va_header=0x01 (1B)] [VARTAG (1B)] [varatt_external (16B)]
// varatt_external: va_rawsize(4) + va_extsize(4) + va_valueid(4) + va_toastrelid(4)
func ParseTOASTPointer(data []byte) *TOASTPointer {
	if len(data) < 18 {
		return nil
	}

	// First byte must be the external varlena header (0x01)
	if data[0] != 0x01 {
		return nil
	}

	// Second byte is the VARTAG: VARTAG_ONDISK = 18 (0x12)
	vartag := data[1]
	if vartag != 0x12 {
		return nil
	}

	// varatt_external structure starts at byte 2 (after header + VARTAG)
	offset := 2

	ptr := &TOASTPointer{}

	// va_rawsize: original data size (includes varlena header)
	ptr.RawSize = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// va_extinfo: compression method in high 2 bits, external stored size in low 30 bits
	extinfo := binary.LittleEndian.Uint32(data[offset : offset+4])
	ptr.ExtSize = extinfo & 0x3FFFFFFF
	// Compression method: 0=pglz, 1=lz4, 2=uncompressed (TOAST_INVALID_COMPRESSION_ID)
	ptr.CompressionMethod = int(extinfo >> 30)
	ptr.IsCompressed = ptr.CompressionMethod < 2
	offset += 4

	// va_valueid (chunk_id)
	ptr.ValueID = binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	// va_toastrelid
	ptr.ToastRelID = binary.LittleEndian.Uint32(data[offset : offset+4])

	return ptr
}

// IsTOASTPointer checks if data is a TOAST pointer
func IsTOASTPointer(data []byte) bool {
	if len(data) < 18 {
		return false
	}
	// External varlena header (0x01) followed by VARTAG_ONDISK (0x12)
	return data[0] == 0x01 && data[1] == 0x12
}

// ReadTOASTTable reads all chunks from a TOAST table file.
// It uses strict visibility: tuples with a non-zero xmax are considered dead
// even if hint bits have not been set yet. This avoids reading stale chunks
// from deleted-but-not-yet-hinted tuples (common after UPDATE before checkpoint).
func ReadTOASTTable(data []byte) []TOASTChunk {
	var chunks []TOASTChunk

	// TOAST table schema:
	// chunk_id (oid/4), chunk_seq (int4/4), chunk_data (bytea/varlena)
	for _, entry := range ReadTuples(data, false) {
		tuple := entry.Tuple
		if tuple == nil || len(tuple.Data) < 8 {
			continue
		}

		// TOAST visibility: accept tuples that are not provably dead.
		// After VACUUM FULL, PostgreSQL rewrites tuples with a new xmin
		// but does not set XMIN_COMMITTED hint bits until a backend reads
		// them through normal access paths. We cannot require XMIN_COMMITTED
		// or we would miss all TOAST data after VACUUM FULL.
		//
		// Instead, skip tuples that are provably dead:
		//   - XMAX_COMMITTED set and XMAX_INVALID not set = definitely deleted
		//   - xmax != 0 and not XMAX_INVALID = targeted by DELETE/UPDATE
		h := tuple.Header
		if h.XmaxCommitted && !h.XmaxInvalid {
			continue
		}
		if h.Xmax != 0 && !h.XmaxInvalid {
			continue
		}

		chunk := TOASTChunk{}
		offset := 0

		// chunk_id (oid, 4 bytes)
		chunk.ChunkID = u32(tuple.Data, offset)
		offset += 4

		// chunk_seq (int4, 4 bytes)
		chunk.ChunkSeq = i32(tuple.Data, offset)
		offset += 4

		// chunk_data (bytea, varlena)
		offset = align(offset, 4)
		if offset < len(tuple.Data) {
			chunkData, _ := ReadVarlena(tuple.Data[offset:])
			chunk.Data = chunkData
		}

		if len(chunk.Data) > 0 {
			chunks = append(chunks, chunk)
		}
	}

	return chunks
}

// ReassembleTOAST reconstructs a value from TOAST chunks
func ReassembleTOAST(chunks []TOASTChunk, valueID uint32, ptr *TOASTPointer) []byte {
	// Filter and sort chunks for this value
	var valueChunks []TOASTChunk
	for _, c := range chunks {
		if c.ChunkID == valueID {
			valueChunks = append(valueChunks, c)
		}
	}

	if len(valueChunks) == 0 {
		return nil
	}

	// Sort by sequence number
	sort.Slice(valueChunks, func(i, j int) bool {
		return valueChunks[i].ChunkSeq < valueChunks[j].ChunkSeq
	})

	// Concatenate all chunks
	var buf bytes.Buffer
	for _, c := range valueChunks {
		buf.Write(c.Data)
	}

	data := buf.Bytes()

	// Decompress if needed
	if ptr != nil && ptr.IsCompressed && len(data) > 4 {
		rawSize := int(ptr.RawSize) - 4 // subtract VARHDRSZ (va_rawsize includes varlena header)

		// TOAST external chunks include a 4-byte va_tcinfo prefix (compression
		// method + raw size) before the actual compressed stream. Skip it for
		// both LZ4 and pglz.
		compressed := data[4:]

		if ptr.CompressionMethod == ToastCompressionLZ4 {
			if d, err := decompressInlineLZ4(compressed, rawSize); err == nil {
				return d
			}
		}

		if d, err := decompressPGLZ(compressed, rawSize); err == nil && len(d) > 0 {
			return d
		}

		// Try zlib as fallback
		if r, err := zlib.NewReader(bytes.NewReader(compressed)); err == nil {
			defer r.Close()
			if d, err := io.ReadAll(r); err == nil {
				return d
			}
		}

		return data
	}

	return data
}

// decompressPGLZ decompresses PostgreSQL's pglz format
func decompressPGLZ(data []byte, rawSize int) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("data too short")
	}

	result := make([]byte, 0, rawSize)
	pos := 0

	for pos < len(data) && len(result) < rawSize {
		ctrl := data[pos]
		pos++

		for bit := 0; bit < 8 && pos < len(data) && len(result) < rawSize; bit++ {
			if ctrl&(1<<bit) != 0 {
				// Back-reference
				if pos+1 >= len(data) {
					break
				}
				b1, b2 := data[pos], data[pos+1]
				pos += 2

				// PostgreSQL pglz format (pg_lzcompress.c):
				// sp[0] low nibble = match length - 3
				// sp[0] high nibble << 4 | sp[1] = back-reference offset
				length := int(b1&0x0F) + 3
				offset := (int(b1&0xF0) << 4) | int(b2)

				// Extended length: if length == 18 (max for 4 bits + 3),
				// next byte is added to get longer matches
				if length == 18 && pos < len(data) {
					length += int(data[pos])
					pos++
				}

				if offset == 0 || offset > len(result) {
					continue
				}

				start := len(result) - offset
				for i := 0; i < length && len(result) < rawSize; i++ {
					result = append(result, result[start+i%offset])
				}
			} else {
				result = append(result, data[pos])
				pos++
			}
		}
	}

	return result, nil
}

// decompressInlineLZ4 decompresses an inline compressed LZ4 varlena.
// Unlike TOAST LZ4, there is no 4-byte size prefix - data is the raw LZ4 block.
func decompressInlineLZ4(data []byte, rawSize int) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty LZ4 data")
	}
	result := make([]byte, rawSize)
	n, err := lz4.UncompressBlock(data, result)
	if err != nil {
		return nil, fmt.Errorf("LZ4 inline decompress failed: %w", err)
	}
	return result[:n], nil
}

// decompressLZ4 decompresses PostgreSQL's LZ4-compressed TOAST data.
// PostgreSQL prepends a 4-byte little-endian raw size before the LZ4 block data.
func decompressLZ4(data []byte, rawSize int) ([]byte, error) {
	if len(data) < 5 {
		return nil, fmt.Errorf("LZ4 data too short: %d bytes", len(data))
	}

	// Skip PostgreSQL's 4-byte size prefix
	compressed := data[4:]

	result := make([]byte, rawSize)
	n, err := lz4.UncompressBlock(compressed, result)
	if err != nil {
		return nil, fmt.Errorf("LZ4 decompress failed: %w", err)
	}

	return result[:n], nil
}

// TOASTReader provides TOAST-aware value reading
type TOASTReader struct {
	chunks   map[uint32][]TOASTChunk // keyed by ToastRelID
	dataDir  string
	dbOID    uint32
}

// NewTOASTReader creates a new TOAST reader
func NewTOASTReader() *TOASTReader {
	return &TOASTReader{
		chunks: make(map[uint32][]TOASTChunk),
	}
}

// NewTOASTReaderForDB creates a TOAST reader for a specific database
func NewTOASTReaderForDB(dataDir string, dbOID uint32) *TOASTReader {
	return &TOASTReader{
		chunks:  make(map[uint32][]TOASTChunk),
		dataDir: dataDir,
		dbOID:   dbOID,
	}
}

// LoadTOASTTable loads chunks from a TOAST table
func (r *TOASTReader) LoadTOASTTable(toastRelID uint32, data []byte) {
	r.chunks[toastRelID] = ReadTOASTTable(data)
}

// LoadTOASTTableFromFile loads a TOAST table from the data directory
func (r *TOASTReader) LoadTOASTTableFromFile(toastRelID uint32) error {
	if r.dataDir == "" {
		return fmt.Errorf("data directory not set")
	}
	
	basePath := filepath.Join(r.dataDir, "base", strconv.FormatUint(uint64(r.dbOID), 10))
	toastPath := filepath.Join(basePath, strconv.FormatUint(uint64(toastRelID), 10))
	
	data, err := os.ReadFile(toastPath)
	if err != nil {
		return err
	}
	
	r.LoadTOASTTable(toastRelID, data)
	return nil
}

// ReadValue reads a value, resolving TOAST pointers if needed
func (r *TOASTReader) ReadValue(data []byte) []byte {
	ptr := ParseTOASTPointer(data)
	if ptr == nil {
		return data
	}

	// Try to load TOAST table if not already loaded
	if _, ok := r.chunks[ptr.ToastRelID]; !ok {
		if r.dataDir != "" {
			r.LoadTOASTTableFromFile(ptr.ToastRelID)
		}
	}

	chunks, ok := r.chunks[ptr.ToastRelID]
	if !ok {
		return nil
	}

	return ReassembleTOAST(chunks, ptr.ValueID, ptr)
}

// GetTOASTInfo returns information about TOAST pointers in a table
type TOASTInfo struct {
	TableName    string   `json:"table_name"`
	ToastRelID   uint32   `json:"toast_rel_id"`
	TotalChunks  int      `json:"total_chunks"`
	UniqueValues int      `json:"unique_values"`
	TotalSize    int64    `json:"total_size"`
}

// TOASTVerboseInfo contains detailed TOAST information
type TOASTVerboseInfo struct {
	ToastRelID        uint32            `json:"toast_rel_id"`
	TotalChunks       int               `json:"total_chunks"`
	UniqueValues      int               `json:"unique_values"`
	TotalSize         int64             `json:"total_size"`
	AverageChunkSize  float64           `json:"average_chunk_size"`
	MaxChunksPerValue int               `json:"max_chunks_per_value"`
	CompressionStats  CompressionStats  `json:"compression_stats"`
	ChunkDistribution map[int]int       `json:"chunk_distribution"` // chunks per value -> count
	Values            []TOASTValueInfo  `json:"values,omitempty"`
}

// CompressionStats contains TOAST compression statistics
type CompressionStats struct {
	Compressed      int   `json:"compressed"`
	Uncompressed    int   `json:"uncompressed"`
	TotalRawSize    int64 `json:"total_raw_size"`
	TotalExtSize    int64 `json:"total_ext_size"`
	CompressionPct  float64 `json:"compression_percent"`
}

// TOASTValueInfo contains info about a single TOAST value
type TOASTValueInfo struct {
	ChunkID       uint32 `json:"chunk_id"`
	NumChunks     int    `json:"num_chunks"`
	TotalSize     int64  `json:"total_size"`
	IsCompressed  bool   `json:"is_compressed"`
	RawSize       uint32 `json:"raw_size,omitempty"`
	ExtSize       uint32 `json:"ext_size,omitempty"`
}

// GetTOASTVerboseInfo returns detailed TOAST information
func GetTOASTVerboseInfo(toastRelID uint32, data []byte) *TOASTVerboseInfo {
	chunks := ReadTOASTTable(data)
	if len(chunks) == 0 {
		return nil
	}

	info := &TOASTVerboseInfo{
		ToastRelID:        toastRelID,
		TotalChunks:       len(chunks),
		ChunkDistribution: make(map[int]int),
	}

	// Group chunks by value ID
	valueChunks := make(map[uint32][]TOASTChunk)
	for _, c := range chunks {
		valueChunks[c.ChunkID] = append(valueChunks[c.ChunkID], c)
		info.TotalSize += int64(len(c.Data))
	}

	info.UniqueValues = len(valueChunks)

	if info.TotalChunks > 0 {
		info.AverageChunkSize = float64(info.TotalSize) / float64(info.TotalChunks)
	}

	// Analyze each value
	for chunkID, chunks := range valueChunks {
		numChunks := len(chunks)
		
		// Update max chunks per value
		if numChunks > info.MaxChunksPerValue {
			info.MaxChunksPerValue = numChunks
		}
		
		// Update distribution
		info.ChunkDistribution[numChunks]++
		
		// Calculate value size
		var valueSize int64
		for _, c := range chunks {
			valueSize += int64(len(c.Data))
		}
		
		info.Values = append(info.Values, TOASTValueInfo{
			ChunkID:   chunkID,
			NumChunks: numChunks,
			TotalSize: valueSize,
		})
	}

	return info
}

// AnalyzeTOAST analyzes TOAST usage for a database
func AnalyzeTOAST(dataDir, dbName string) ([]TOASTInfo, error) {
	// Find database OID
	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, err
	}
	
	dbOID := FindDatabaseOID(dbData, dbName)
	if dbOID == 0 {
		return nil, fmt.Errorf("database %q not found", dbName)
	}

	basePath := filepath.Join(dataDir, "base", strconv.FormatUint(uint64(dbOID), 10))

	// Read pg_class to find TOAST tables
	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		return nil, err
	}
	
	var results []TOASTInfo
	
	// Parse pg_class with extended schema to get reltoastrelid
	for _, entry := range ReadTuples(classData, true) {
		tuple := entry.Tuple
		if tuple == nil || len(tuple.Data) < 60 {
			continue
		}
		
		// Check if this table has a TOAST table (reltoastrelid at offset ~48-52)
		// This is approximate - proper parsing would use full schema
		toastRelID := u32(tuple.Data, 48)
		if toastRelID == 0 {
			continue
		}
		
		// Try to read the TOAST table
		toastPath := filepath.Join(basePath, strconv.FormatUint(uint64(toastRelID), 10))
		toastData, err := os.ReadFile(toastPath)
		if err != nil {
			continue
		}
		
		chunks := ReadTOASTTable(toastData)
		if len(chunks) == 0 {
			continue
		}
		
		// Count unique values
		uniqueValues := make(map[uint32]bool)
		var totalSize int64
		for _, c := range chunks {
			uniqueValues[c.ChunkID] = true
			totalSize += int64(len(c.Data))
		}
		
		results = append(results, TOASTInfo{
			ToastRelID:   toastRelID,
			TotalChunks:  len(chunks),
			UniqueValues: len(uniqueValues),
			TotalSize:    totalSize,
		})
	}
	
	return results, nil
}
