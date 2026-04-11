package pgdump

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// SequenceMagic is the magic number for sequence pages
const SequenceMagic = 0x1717

// SequenceData represents a PostgreSQL sequence
type SequenceData struct {
	Name        string `json:"name,omitempty"`
	OID         uint32 `json:"oid,omitempty"`
	Filenode    uint32 `json:"filenode,omitempty"`
	LastValue   int64  `json:"last_value"`
	StartValue  int64  `json:"start_value"`
	IncrementBy int64  `json:"increment_by"`
	MaxValue    int64  `json:"max_value"`
	MinValue    int64  `json:"min_value"`
	CacheValue  int64  `json:"cache_value"`
	IsCycled    bool   `json:"is_cycled"`
	IsCalled    bool   `json:"is_called"`
}

// ParseSequenceFile parses a PostgreSQL sequence file
// Sequence files have a special page format with magic number 0x1717
func ParseSequenceFile(data []byte) (*SequenceData, error) {
	if len(data) < PageSize {
		return nil, fmt.Errorf("sequence file too small: %d bytes", len(data))
	}

	// Check page header
	// pd_special points to the sequence magic number at end of page
	special := binary.LittleEndian.Uint16(data[16:18])
	if special == 0 || int(special) >= PageSize-2 {
		return nil, fmt.Errorf("invalid special pointer")
	}

	// Check magic number at special section
	magic := binary.LittleEndian.Uint16(data[special:])
	if magic != SequenceMagic {
		return nil, fmt.Errorf("not a sequence file (magic: 0x%04X, expected: 0x%04X)", magic, SequenceMagic)
	}

	// Sequence data is stored as a tuple on the page
	// Find the first item pointer
	lower := binary.LittleEndian.Uint16(data[12:14])
	if lower < headerSize+itemIDSize {
		return nil, fmt.Errorf("no items on page")
	}

	// Read first item pointer
	itemPtr := binary.LittleEndian.Uint32(data[headerSize : headerSize+4])
	itemOffset := int(itemPtr & 0x7FFF)
	itemLen := int((itemPtr >> 17) & 0x7FFF)

	if itemOffset == 0 || itemLen == 0 || itemOffset+itemLen > PageSize {
		return nil, fmt.Errorf("invalid item pointer")
	}

	// Parse the tuple containing sequence data
	tupleData := data[itemOffset : itemOffset+itemLen]
	
	// Skip heap tuple header (23 bytes minimum)
	if len(tupleData) < 23 {
		return nil, fmt.Errorf("tuple too small")
	}

	// HeapTupleHeaderData:
	// t_xmin (4), t_xmax (4), t_cid/t_xvac (4), t_ctid (6), 
	// t_infomask2 (2), t_infomask (2), t_hoff (1)
	hoff := int(tupleData[22])
	if hoff < 23 || hoff > len(tupleData) {
		hoff = 24 // Default aligned offset
	}

	// Sequence tuple data starts after header
	seqData := tupleData[hoff:]
	
	return parseSequenceTuple(seqData)
}

// parseSequenceTuple parses the sequence tuple data
// PostgreSQL sequence tuple format (FormData_pg_sequence):
// seqtypid (4), seqstart (8), seqincrement (8), seqmax (8), seqmin (8), seqcache (8), seqcycle (1)
// Plus the SEQ_LOG_VALS section: last_value (8), is_called (1)
func parseSequenceTuple(data []byte) (*SequenceData, error) {
	seq := &SequenceData{}

	// For older PostgreSQL versions, the format is different
	// Let's try to detect based on data patterns

	if len(data) < 8 {
		return nil, fmt.Errorf("sequence data too short")
	}

	offset := 0

	// Try to determine format by looking at the data
	// Modern format (PG 10+): seqtypid, seqstart, seqincrement, seqmax, seqmin, seqcache, seqcycle
	// Plus runtime: last_value, log_cnt, is_called

	// Check if first 4 bytes look like a type OID (20=int8, 21=int2, 23=int4)
	firstVal := binary.LittleEndian.Uint32(data[0:4])
	
	if firstVal == 20 || firstVal == 21 || firstVal == 23 {
		// Modern format (PG 10+)
		offset = 4 // Skip seqtypid
		
		if len(data) < offset+48 {
			return nil, fmt.Errorf("sequence data too short for modern format")
		}

		seq.StartValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.IncrementBy = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.MaxValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.MinValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.CacheValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.IsCycled = data[offset] != 0
		offset++

		// Align to 8 bytes for runtime data
		offset = (offset + 7) &^ 7

		if len(data) >= offset+9 {
			seq.LastValue = int64(binary.LittleEndian.Uint64(data[offset:]))
			offset += 8
			// Skip log_cnt
			if len(data) > offset {
				seq.IsCalled = data[offset] != 0
			}
		}
	} else {
		// Old format (PG 9.x and earlier) or runtime-only data
		// Format: last_value (8), start_value (8), increment_by (8), max_value (8), 
		//         min_value (8), cache_value (8), log_cnt (8), is_cycled (1), is_called (1)
		
		if len(data) < 57 {
			// Minimal format: just last_value
			if len(data) >= 8 {
				seq.LastValue = int64(binary.LittleEndian.Uint64(data[0:8]))
			}
			return seq, nil
		}

		seq.LastValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.StartValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.IncrementBy = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.MaxValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.MinValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		seq.CacheValue = int64(binary.LittleEndian.Uint64(data[offset:]))
		offset += 8

		// log_cnt
		offset += 8

		if len(data) > offset {
			seq.IsCycled = data[offset] != 0
			offset++
		}
		if len(data) > offset {
			seq.IsCalled = data[offset] != 0
		}
	}

	return seq, nil
}

// IsSequenceFile checks if a file is a sequence file
func IsSequenceFile(data []byte) bool {
	if len(data) < PageSize {
		return false
	}

	special := binary.LittleEndian.Uint16(data[16:18])
	if special == 0 || int(special) >= PageSize-2 {
		return false
	}

	magic := binary.LittleEndian.Uint16(data[special:])
	return magic == SequenceMagic
}

// FindSequences finds all sequences in a database
func FindSequences(dataDir, dbName string) ([]SequenceData, error) {
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

	// Read pg_class to find sequences (relkind = 'S')
	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		return nil, err
	}

	tables := ParsePGClass(classData)
	var sequences []SequenceData

	for filenode, info := range tables {
		// Check if it's a sequence (relkind = 'S')
		if info.Kind != "S" {
			continue
		}

		// Read the sequence file
		seqPath := filepath.Join(basePath, strconv.FormatUint(uint64(filenode), 10))
		seqData, err := os.ReadFile(seqPath)
		if err != nil {
			continue
		}

		seq, err := ParseSequenceFile(seqData)
		if err != nil {
			continue
		}

		seq.Name = info.Name
		seq.OID = info.OID
		seq.Filenode = filenode
		sequences = append(sequences, *seq)
	}

	return sequences, nil
}

// ScanAllSequences scans all databases for sequences
func ScanAllSequences(dataDir string) (map[string][]SequenceData, error) {
	results := make(map[string][]SequenceData)

	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, err
	}

	for _, db := range ParsePGDatabase(dbData) {
		if isTemplateDB(db.Name) {
			continue
		}

		seqs, err := FindSequences(dataDir, db.Name)
		if err != nil {
			continue
		}

		if len(seqs) > 0 {
			results[db.Name] = seqs
		}
	}

	return results, nil
}
