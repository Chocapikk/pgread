package pgdump

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WAL magic numbers by PostgreSQL version
const (
	WAL_MAGIC_16 = 0xD113 // PostgreSQL 16
	WAL_MAGIC_15 = 0xD110 // PostgreSQL 15
	WAL_MAGIC_14 = 0xD10F // PostgreSQL 14
	WAL_MAGIC_13 = 0xD10D // PostgreSQL 13
	WAL_MAGIC_12 = 0xD109 // PostgreSQL 12
)

// WAL page constants
const (
	WALPageSize     = 8192
	XLogRecordSize  = 24 // sizeof(XLogRecord)
	ShortHeaderSize = 24 // sizeof(XLogPageHeaderData)
	LongHeaderSize  = 40 // sizeof(XLogLongPageHeaderData)
)

// Page info flags
const (
	XLP_FIRST_IS_CONTRECORD = 0x0001
	XLP_LONG_HEADER         = 0x0002
	XLP_BKP_REMOVABLE       = 0x0004
)

// Resource manager IDs (from rmgrlist.h)
const (
	RM_XLOG_ID      = 0
	RM_XACT_ID      = 1  // Transaction commit/abort
	RM_SMGR_ID      = 2  // Storage manager
	RM_CLOG_ID      = 3  // Commit log
	RM_DBASE_ID     = 4  // Database
	RM_TBLSPC_ID    = 5  // Tablespace
	RM_MULTIXACT_ID = 6  // MultiXact
	RM_RELMAP_ID    = 7  // Relation map
	RM_STANDBY_ID   = 8  // Standby
	RM_HEAP2_ID     = 9  // Heap2 (HOT, freeze, etc)
	RM_HEAP_ID      = 10 // Heap (insert/update/delete)
	RM_BTREE_ID     = 11 // BTree
	RM_HASH_ID      = 12 // Hash
	RM_GIN_ID       = 13 // GIN
	RM_GIST_ID      = 14 // GiST
	RM_SEQ_ID       = 15 // Sequence
	RM_SPGIST_ID    = 16 // SP-GiST
	RM_BRIN_ID      = 17 // BRIN
	RM_COMMIT_TS_ID = 18 // Commit timestamp
	RM_REPLORIGIN_ID= 19 // Replication origin
	RM_GENERIC_ID   = 20 // Generic WAL
	RM_LOGICALMSG_ID= 21 // Logical messages
)

// Heap operation info bits
const (
	XLOG_HEAP_INSERT       = 0x00
	XLOG_HEAP_DELETE       = 0x10
	XLOG_HEAP_UPDATE       = 0x20
	XLOG_HEAP_TRUNCATE     = 0x30
	XLOG_HEAP_HOT_UPDATE   = 0x40
	XLOG_HEAP_CONFIRM      = 0x50
	XLOG_HEAP_LOCK         = 0x60
	XLOG_HEAP_INPLACE      = 0x70
)

// Transaction operation info bits
const (
	XLOG_XACT_COMMIT          = 0x00
	XLOG_XACT_PREPARE         = 0x10
	XLOG_XACT_ABORT           = 0x20
	XLOG_XACT_COMMIT_PREPARED = 0x30
	XLOG_XACT_ABORT_PREPARED  = 0x40
	XLOG_XACT_ASSIGNMENT      = 0x50
)

// WALPageHeader represents XLogPageHeaderData
type WALPageHeader struct {
	Magic       uint16 `json:"magic"`
	Info        uint16 `json:"info"`
	TimelineID  uint32 `json:"timeline_id"`
	PageAddr    uint64 `json:"page_addr"`
	RemLen      uint32 `json:"remaining_len"`
	// Long header fields (if XLP_LONG_HEADER)
	SystemID    uint64 `json:"system_id,omitempty"`
	SegSize     uint32 `json:"seg_size,omitempty"`
	BlockSize   uint32 `json:"block_size,omitempty"`
}

// WALRecord represents XLogRecord
type WALRecord struct {
	TotalLen      uint32 `json:"total_len"`
	TransactionID uint32 `json:"xid"`
	PrevLSN       uint64 `json:"prev_lsn"`
	Info          uint8  `json:"info"`
	ResourceMgr   uint8  `json:"rmid"`
	CRC           uint32 `json:"crc"`
	// Parsed fields
	LSN           uint64 `json:"lsn"`
	RMName        string `json:"rm_name"`
	Operation     string `json:"operation"`
	// Block references
	Blocks        []WALBlockRef `json:"blocks,omitempty"`
}

// WALBlockRef represents a block reference in a WAL record
type WALBlockRef struct {
	ID          uint8  `json:"id"`
	ForkNum     uint8  `json:"fork_num"`
	Flags       uint16 `json:"flags"`
	RelFileNode *RelFileNode `json:"relfilenode,omitempty"`
	BlockNum    uint32 `json:"block_num"`
}

// RelFileNode identifies a relation file
type RelFileNode struct {
	SpcOID uint32 `json:"spcoid"` // Tablespace OID
	DbOID  uint32 `json:"dboid"`  // Database OID
	RelOID uint32 `json:"reloid"` // Relation filenode
}

// WALSummary summarizes WAL contents
type WALSummary struct {
	SegmentCount   int                 `json:"segment_count"`
	RecordCount    int                 `json:"record_count"`
	FirstLSN       string              `json:"first_lsn"`
	LastLSN        string              `json:"last_lsn"`
	PGVersion      string              `json:"pg_version"`
	TimelineID     uint32              `json:"timeline_id"`
	Operations     map[string]int      `json:"operations"`
	Transactions   []TransactionInfo   `json:"transactions,omitempty"`
	AffectedTables map[string]int      `json:"affected_tables"`
}

// TransactionInfo describes a transaction in WAL
type TransactionInfo struct {
	XID       uint32 `json:"xid"`
	Status    string `json:"status"` // COMMIT, ABORT, IN_PROGRESS
	Operations int   `json:"operations"`
}

// ParseWALFile parses a single WAL segment file
func ParseWALFile(data []byte) ([]WALRecord, error) {
	if len(data) < LongHeaderSize {
		return nil, fmt.Errorf("WAL file too small")
	}

	var records []WALRecord
	pageNum := 0

	for offset := 0; offset+WALPageSize <= len(data); offset += WALPageSize {
		pageData := data[offset : offset+WALPageSize]
		pageRecords, err := parseWALPage(pageData, uint64(offset), pageNum)
		if err != nil {
			continue // Skip invalid pages
		}
		records = append(records, pageRecords...)
		pageNum++
	}

	return records, nil
}

func parseWALPage(data []byte, baseOffset uint64, pageNum int) ([]WALRecord, error) {
	if len(data) < ShortHeaderSize {
		return nil, fmt.Errorf("page too small")
	}

	header := parsePageHeader(data)
	
	// Validate magic
	if !isValidMagic(header.Magic) {
		return nil, fmt.Errorf("invalid magic: 0x%04X", header.Magic)
	}

	headerSize := ShortHeaderSize
	if header.Info&XLP_LONG_HEADER != 0 {
		headerSize = LongHeaderSize
	}

	var records []WALRecord
	pos := headerSize

	// If this page continues a record from previous page, skip the continuation
	if header.Info&XLP_FIRST_IS_CONTRECORD != 0 && header.RemLen > 0 {
		pos += int(header.RemLen)
		pos = align8(pos)
	}

	// Parse records
	for pos+XLogRecordSize <= len(data) {
		// Check for padding (zeros)
		if isZeroPadding(data[pos:]) {
			break
		}

		rec, consumed := parseXLogRecord(data[pos:], baseOffset+uint64(pos))
		if consumed == 0 {
			break
		}

		if rec != nil {
			records = append(records, *rec)
		}

		pos += consumed
		pos = align8(pos)
	}

	return records, nil
}

func parsePageHeader(data []byte) *WALPageHeader {
	h := &WALPageHeader{
		Magic:      binary.LittleEndian.Uint16(data[0:2]),
		Info:       binary.LittleEndian.Uint16(data[2:4]),
		TimelineID: binary.LittleEndian.Uint32(data[4:8]),
		PageAddr:   binary.LittleEndian.Uint64(data[8:16]),
		RemLen:     binary.LittleEndian.Uint32(data[16:20]),
	}

	if h.Info&XLP_LONG_HEADER != 0 && len(data) >= LongHeaderSize {
		h.SystemID = binary.LittleEndian.Uint64(data[24:32])
		h.SegSize = binary.LittleEndian.Uint32(data[32:36])
		h.BlockSize = binary.LittleEndian.Uint32(data[36:40])
	}

	return h
}

func parseXLogRecord(data []byte, lsn uint64) (*WALRecord, int) {
	if len(data) < XLogRecordSize {
		return nil, 0
	}

	totalLen := binary.LittleEndian.Uint32(data[0:4])
	if totalLen < XLogRecordSize || totalLen > WALPageSize*2 {
		return nil, 0
	}

	rec := &WALRecord{
		TotalLen:      totalLen,
		TransactionID: binary.LittleEndian.Uint32(data[4:8]),
		PrevLSN:       binary.LittleEndian.Uint64(data[8:16]),
		Info:          data[16],
		ResourceMgr:   data[17],
		CRC:           binary.LittleEndian.Uint32(data[20:24]),
		LSN:           lsn,
	}

	rec.RMName = rmgrName(rec.ResourceMgr)
	rec.Operation = operationName(rec.ResourceMgr, rec.Info)

	// Parse block references if present
	if int(totalLen) > XLogRecordSize && int(totalLen) <= len(data) {
		rec.Blocks = parseBlockRefs(data[XLogRecordSize:totalLen])
	}

	return rec, int(totalLen)
}

func parseBlockRefs(data []byte) []WALBlockRef {
	var blocks []WALBlockRef
	pos := 0

	for pos < len(data) {
		if pos+1 > len(data) {
			break
		}

		blockID := data[pos]
		pos++

		// Check for XLR_BLOCK_ID_DATA_SHORT or XLR_BLOCK_ID_DATA_LONG
		if blockID == 0xFF || blockID == 0xFE {
			break // End of block references
		}

		if blockID > 32 {
			break // Invalid block ID
		}

		if pos+1 > len(data) {
			break
		}

		forkFlags := data[pos]
		pos++

		block := WALBlockRef{
			ID:      blockID,
			ForkNum: forkFlags & 0x0F,
			Flags:   uint16(forkFlags),
		}

		// Check if BKPBLOCK_HAS_DATA
		hasImage := (forkFlags & 0x10) != 0
		hasData := (forkFlags & 0x20) != 0
		hasSameRel := (forkFlags & 0x40) != 0

		if !hasSameRel && pos+12 <= len(data) {
			block.RelFileNode = &RelFileNode{
				SpcOID: binary.LittleEndian.Uint32(data[pos : pos+4]),
				DbOID:  binary.LittleEndian.Uint32(data[pos+4 : pos+8]),
				RelOID: binary.LittleEndian.Uint32(data[pos+8 : pos+12]),
			}
			pos += 12
		}

		if pos+4 <= len(data) {
			block.BlockNum = binary.LittleEndian.Uint32(data[pos : pos+4])
			pos += 4
		}

		// Skip image data
		if hasImage && pos+2 <= len(data) {
			imageLen := binary.LittleEndian.Uint16(data[pos : pos+2])
			pos += 2 + int(imageLen)
		}

		// Skip block data
		if hasData && pos+2 <= len(data) {
			dataLen := binary.LittleEndian.Uint16(data[pos : pos+2])
			pos += 2 + int(dataLen)
		}

		blocks = append(blocks, block)
	}

	return blocks
}

func isValidMagic(magic uint16) bool {
	switch magic {
	case WAL_MAGIC_16, WAL_MAGIC_15, WAL_MAGIC_14, WAL_MAGIC_13, WAL_MAGIC_12:
		return true
	}
	return false
}

func pgVersionFromMagic(magic uint16) string {
	switch magic {
	case WAL_MAGIC_16:
		return "16"
	case WAL_MAGIC_15:
		return "15"
	case WAL_MAGIC_14:
		return "14"
	case WAL_MAGIC_13:
		return "13"
	case WAL_MAGIC_12:
		return "12"
	}
	return "unknown"
}

func isZeroPadding(data []byte) bool {
	for i := 0; i < 8 && i < len(data); i++ {
		if data[i] != 0 {
			return false
		}
	}
	return true
}

func align8(n int) int {
	return align(n, 8)
}

func rmgrName(rmid uint8) string {
	names := map[uint8]string{
		RM_XLOG_ID:       "XLOG",
		RM_XACT_ID:       "Transaction",
		RM_SMGR_ID:       "Storage",
		RM_CLOG_ID:       "CLOG",
		RM_DBASE_ID:      "Database",
		RM_TBLSPC_ID:     "Tablespace",
		RM_MULTIXACT_ID:  "MultiXact",
		RM_RELMAP_ID:     "RelMap",
		RM_STANDBY_ID:    "Standby",
		RM_HEAP2_ID:      "Heap2",
		RM_HEAP_ID:       "Heap",
		RM_BTREE_ID:      "BTree",
		RM_HASH_ID:       "Hash",
		RM_GIN_ID:        "GIN",
		RM_GIST_ID:       "GiST",
		RM_SEQ_ID:        "Sequence",
		RM_SPGIST_ID:     "SP-GiST",
		RM_BRIN_ID:       "BRIN",
		RM_COMMIT_TS_ID:  "CommitTS",
		RM_REPLORIGIN_ID: "ReplOrigin",
		RM_GENERIC_ID:    "Generic",
		RM_LOGICALMSG_ID: "LogicalMsg",
	}
	if name, ok := names[rmid]; ok {
		return name
	}
	return fmt.Sprintf("RM_%d", rmid)
}

func operationName(rmid, info uint8) string {
	switch rmid {
	case RM_HEAP_ID:
		switch info & 0x70 {
		case XLOG_HEAP_INSERT:
			return "INSERT"
		case XLOG_HEAP_DELETE:
			return "DELETE"
		case XLOG_HEAP_UPDATE:
			return "UPDATE"
		case XLOG_HEAP_TRUNCATE:
			return "TRUNCATE"
		case XLOG_HEAP_HOT_UPDATE:
			return "HOT_UPDATE"
		case XLOG_HEAP_CONFIRM:
			return "CONFIRM"
		case XLOG_HEAP_LOCK:
			return "LOCK"
		case XLOG_HEAP_INPLACE:
			return "INPLACE"
		}
	case RM_HEAP2_ID:
		switch info & 0x70 {
		case 0x00:
			return "PRUNE"
		case 0x10:
			return "VACUUM"
		case 0x20:
			return "FREEZE_PAGE"
		case 0x30:
			return "VISIBLE"
		case 0x40:
			return "MULTI_INSERT"
		case 0x50:
			return "LOCK_UPDATED"
		case 0x60:
			return "NEW_CID"
		}
	case RM_XACT_ID:
		switch info & 0x70 {
		case XLOG_XACT_COMMIT:
			return "COMMIT"
		case XLOG_XACT_PREPARE:
			return "PREPARE"
		case XLOG_XACT_ABORT:
			return "ABORT"
		case XLOG_XACT_COMMIT_PREPARED:
			return "COMMIT_PREPARED"
		case XLOG_XACT_ABORT_PREPARED:
			return "ABORT_PREPARED"
		case XLOG_XACT_ASSIGNMENT:
			return "ASSIGNMENT"
		}
	case RM_XLOG_ID:
		switch info & 0xF0 {
		case 0x00:
			return "CHECKPOINT_SHUTDOWN"
		case 0x10:
			return "CHECKPOINT_ONLINE"
		case 0x20:
			return "NOOP"
		case 0x30:
			return "NEXTOID"
		case 0x40:
			return "SWITCH"
		case 0x50:
			return "BACKUP_END"
		case 0x60:
			return "PARAMETER_CHANGE"
		case 0x70:
			return "RESTORE_POINT"
		case 0x80:
			return "FPW_CHANGE"
		case 0x90:
			return "END_OF_RECOVERY"
		case 0xA0:
			return "OVERWRITE_CONTRECORD"
		}
	case RM_SMGR_ID:
		switch info & 0x70 {
		case 0x10:
			return "CREATE"
		case 0x20:
			return "TRUNCATE"
		}
	case RM_DBASE_ID:
		switch info & 0x70 {
		case 0x00:
			return "CREATE"
		case 0x10:
			return "DROP"
		}
	case RM_BTREE_ID:
		switch info & 0x70 {
		case 0x00:
			return "INSERT_LEAF"
		case 0x10:
			return "INSERT_UPPER"
		case 0x20:
			return "INSERT_META"
		case 0x30:
			return "SPLIT_L"
		case 0x40:
			return "SPLIT_R"
		case 0x60:
			return "DELETE"
		case 0x70:
			return "UNLINK_PAGE"
		}
	}
	return fmt.Sprintf("op_0x%02X", info)
}

// FormatLSN formats an LSN as PostgreSQL does (e.g., "0/1234ABC")
func FormatLSN(lsn uint64) string {
	return fmt.Sprintf("%X/%X", lsn>>32, lsn&0xFFFFFFFF)
}

// ScanWALDirectory scans pg_wal directory and returns summary
func ScanWALDirectory(dataDir string) (*WALSummary, error) {
	walDir := filepath.Join(dataDir, "pg_wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, fmt.Errorf("cannot read pg_wal: %w", err)
	}

	summary := &WALSummary{
		Operations:     make(map[string]int),
		AffectedTables: make(map[string]int),
	}

	txnOps := make(map[uint32]int)
	txnStatus := make(map[uint32]string)
	var firstLSN, lastLSN uint64

	// Sort WAL files by name
	var walFiles []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && len(name) == 24 && !strings.HasSuffix(name, ".history") {
			walFiles = append(walFiles, name)
		}
	}
	sort.Strings(walFiles)

	for _, name := range walFiles {
		data, err := os.ReadFile(filepath.Join(walDir, name))
		if err != nil {
			continue
		}

		records, err := ParseWALFile(data)
		if err != nil {
			continue
		}

		summary.SegmentCount++

		// Get PG version from first page
		if summary.PGVersion == "" && len(data) >= 2 {
			magic := binary.LittleEndian.Uint16(data[0:2])
			summary.PGVersion = pgVersionFromMagic(magic)
		}

		// Get timeline from first page
		if summary.TimelineID == 0 && len(data) >= 8 {
			summary.TimelineID = binary.LittleEndian.Uint32(data[4:8])
		}

		for _, rec := range records {
			summary.RecordCount++

			if firstLSN == 0 || rec.LSN < firstLSN {
				firstLSN = rec.LSN
			}
			if rec.LSN > lastLSN {
				lastLSN = rec.LSN
			}

			summary.Operations[rec.Operation]++

			// Track transactions
			if rec.TransactionID != 0 {
				txnOps[rec.TransactionID]++
				if rec.ResourceMgr == RM_XACT_ID {
					if strings.Contains(rec.Operation, "COMMIT") {
						txnStatus[rec.TransactionID] = "COMMIT"
					} else if strings.Contains(rec.Operation, "ABORT") {
						txnStatus[rec.TransactionID] = "ABORT"
					}
				}
			}

			// Track affected tables
			for _, block := range rec.Blocks {
				if block.RelFileNode != nil && block.RelFileNode.RelOID != 0 {
					key := fmt.Sprintf("%d/%d", block.RelFileNode.DbOID, block.RelFileNode.RelOID)
					summary.AffectedTables[key]++
				}
			}
		}
	}

	summary.FirstLSN = FormatLSN(firstLSN)
	summary.LastLSN = FormatLSN(lastLSN)

	// Build transaction list
	for xid, ops := range txnOps {
		status := txnStatus[xid]
		if status == "" {
			status = "IN_PROGRESS"
		}
		summary.Transactions = append(summary.Transactions, TransactionInfo{
			XID:        xid,
			Status:     status,
			Operations: ops,
		})
	}

	// Sort transactions by XID
	sort.Slice(summary.Transactions, func(i, j int) bool {
		return summary.Transactions[i].XID < summary.Transactions[j].XID
	})

	return summary, nil
}

// GetRecentWALRecords returns the most recent WAL records
func GetRecentWALRecords(dataDir string, limit int) ([]WALRecord, error) {
	walDir := filepath.Join(dataDir, "pg_wal")
	entries, err := os.ReadDir(walDir)
	if err != nil {
		return nil, err
	}

	var walFiles []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && len(name) == 24 && !strings.HasSuffix(name, ".history") {
			walFiles = append(walFiles, name)
		}
	}
	sort.Strings(walFiles)

	var allRecords []WALRecord

	// Read from newest files first
	for i := len(walFiles) - 1; i >= 0 && len(allRecords) < limit; i-- {
		data, err := os.ReadFile(filepath.Join(walDir, walFiles[i]))
		if err != nil {
			continue
		}
		records, err := ParseWALFile(data)
		if err != nil {
			continue
		}
		allRecords = append(records, allRecords...)
	}

	if len(allRecords) > limit {
		allRecords = allRecords[len(allRecords)-limit:]
	}

	return allRecords, nil
}
