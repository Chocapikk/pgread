package pgdump

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"
)

// ControlFile represents PostgreSQL pg_control file
type ControlFile struct {
	// Version info
	PGControlVersion uint32 `json:"pg_control_version"`
	CatalogVersionNo uint32 `json:"catalog_version_no"`
	SystemIdentifier uint64 `json:"system_identifier"`

	// Database state
	State       DBState `json:"state"`
	StateString string  `json:"state_string"`

	// Checkpoint info
	CheckpointLSN    string `json:"checkpoint_lsn"`
	RedoLSN          string `json:"redo_lsn"`
	RedoWALFile      string `json:"redo_wal_file"`
	TimeLineID       uint32 `json:"timeline_id"`
	PrevTimeLineID   uint32 `json:"prev_timeline_id"`
	FullPageWrites   bool   `json:"full_page_writes"`

	// Transaction info
	NextXIDEpoch      uint32 `json:"next_xid_epoch"`
	NextXID           uint32 `json:"next_xid"`
	NextOID           uint32 `json:"next_oid"`
	NextMulti         uint32 `json:"next_multi"`
	NextMultiOffset   uint32 `json:"next_multi_offset"`
	OldestXID         uint32 `json:"oldest_xid"`
	OldestXIDDB       uint32 `json:"oldest_xid_db"`
	OldestActiveXID   uint32 `json:"oldest_active_xid"`
	OldestMulti       uint32 `json:"oldest_multi"`
	OldestMultiDB     uint32 `json:"oldest_multi_db"`
	OldestCommitTsXID uint32 `json:"oldest_commit_ts_xid"`
	NewestCommitTsXID uint32 `json:"newest_commit_ts_xid"`

	// Time info
	CheckpointTime time.Time `json:"checkpoint_time"`

	// Configuration
	WALLevel           string `json:"wal_level"`
	WALLogHints        bool   `json:"wal_log_hints"`
	MaxConnections     int32  `json:"max_connections"`
	MaxWorkerProcesses int32  `json:"max_worker_processes"`
	MaxWALSenders      int32  `json:"max_wal_senders"`
	MaxPreparedXacts   int32  `json:"max_prepared_xacts"`
	MaxLocksPerXact    int32  `json:"max_locks_per_xact"`
	TrackCommitTS      bool   `json:"track_commit_timestamp"`

	// Storage parameters
	MaxAlign        uint32 `json:"max_align"`
	BlockSize       uint32 `json:"block_size"`
	BlocksPerSeg    uint32 `json:"blocks_per_segment"`
	WALBlockSize    uint32 `json:"wal_block_size"`
	WALSegmentSize  uint32 `json:"wal_segment_size"`
	NameDataLen     uint32 `json:"name_data_len"`
	IndexMaxKeys    uint32 `json:"index_max_keys"`
	TOASTMaxChunk   uint32 `json:"toast_max_chunk_size"`
	LargeObjectChunk uint32 `json:"large_object_chunk_size"`

	// Float format check (should be 1234567.0)
	FloatFormatOK bool `json:"float_format_ok"`

	// Checksums
	DataChecksumsEnabled bool `json:"data_checksums_enabled"`

	// CRC
	CRC      uint32 `json:"crc"`
	CRCValid bool   `json:"crc_valid"`

	// Inferred PostgreSQL version
	PGVersionMajor int `json:"pg_version_major"`
}

// DBState represents database state
type DBState int32

const (
	DBStateStartup DBState = iota
	DBStateShutdowned
	DBStateShutdownedInRecovery
	DBStateShutdowning
	DBStateInCrashRecovery
	DBStateInArchiveRecovery
	DBStateInProduction
)

func (s DBState) String() string {
	switch s {
	case DBStateStartup:
		return "starting up"
	case DBStateShutdowned:
		return "shut down"
	case DBStateShutdownedInRecovery:
		return "shut down in recovery"
	case DBStateShutdowning:
		return "shutting down"
	case DBStateInCrashRecovery:
		return "in crash recovery"
	case DBStateInArchiveRecovery:
		return "in archive recovery"
	case DBStateInProduction:
		return "in production"
	default:
		return fmt.Sprintf("unknown (%d)", s)
	}
}

// WAL level names
var walLevelNames = []string{"minimal", "replica", "logical"}

// ParseControlFile parses pg_control file
// Based on PostgreSQL src/include/catalog/pg_control.h
func ParseControlFile(data []byte) (*ControlFile, error) {
	if len(data) < 296 {
		return nil, fmt.Errorf("control file too small: %d bytes", len(data))
	}

	cf := &ControlFile{}

	// ControlFileData structure (64-bit aligned)
	// Offsets for PostgreSQL 12+

	// system_identifier: uint64 at offset 0
	cf.SystemIdentifier = binary.LittleEndian.Uint64(data[0:8])

	// pg_control_version: uint32 at offset 8
	cf.PGControlVersion = binary.LittleEndian.Uint32(data[8:12])

	// catalog_version_no: uint32 at offset 12
	cf.CatalogVersionNo = binary.LittleEndian.Uint32(data[12:16])

	// Determine PG version
	cf.PGVersionMajor = inferPGVersion(cf.PGControlVersion, cf.CatalogVersionNo)

	// state: DBState (int32) at offset 16
	cf.State = DBState(binary.LittleEndian.Uint32(data[16:20]))
	cf.StateString = cf.State.String()

	// pg_time_t time: int64 at offset 24 (after 4 bytes padding)
	// This is the last modification time of pg_control itself
	// Skip this, we'll use checkpoint time instead

	// checkPoint: XLogRecPtr (uint64) at offset 32
	checkpointLSN := binary.LittleEndian.Uint64(data[32:40])
	cf.CheckpointLSN = formatLSN(checkpointLSN)

	// checkPointCopy starts at offset 40 (CheckPoint structure)
	// CheckPoint.redo: XLogRecPtr at offset 40
	redoLSN := binary.LittleEndian.Uint64(data[40:48])
	cf.RedoLSN = formatLSN(redoLSN)
	cf.RedoWALFile = formatWALFilename(redoLSN, 1) // timeline 1 as default

	// CheckPoint.ThisTimeLineID: uint32 at offset 48
	cf.TimeLineID = binary.LittleEndian.Uint32(data[48:52])

	// CheckPoint.PrevTimeLineID: uint32 at offset 52
	cf.PrevTimeLineID = binary.LittleEndian.Uint32(data[52:56])

	// CheckPoint.fullPageWrites: bool at offset 56
	cf.FullPageWrites = data[56] != 0

	// 7 bytes padding to align nextXid to 8-byte boundary
	// CheckPoint.nextXid: FullTransactionId at offset 64
	// FullTransactionId in little-endian: low 32 bits = xid, high 32 bits = epoch
	cf.NextXID = binary.LittleEndian.Uint32(data[64:68])
	cf.NextXIDEpoch = binary.LittleEndian.Uint32(data[68:72])

	// CheckPoint.nextOid: Oid at offset 72
	cf.NextOID = binary.LittleEndian.Uint32(data[72:76])

	// CheckPoint.nextMulti: MultiXactId at offset 76
	cf.NextMulti = binary.LittleEndian.Uint32(data[76:80])

	// CheckPoint.nextMultiOffset: MultiXactOffset at offset 80
	cf.NextMultiOffset = binary.LittleEndian.Uint32(data[80:84])

	// CheckPoint.oldestXid: TransactionId at offset 84
	cf.OldestXID = binary.LittleEndian.Uint32(data[84:88])

	// CheckPoint.oldestXidDB: Oid at offset 88
	cf.OldestXIDDB = binary.LittleEndian.Uint32(data[88:92])

	// CheckPoint.oldestMulti: MultiXactId at offset 92
	cf.OldestMulti = binary.LittleEndian.Uint32(data[92:96])

	// CheckPoint.oldestMultiDB: Oid at offset 96
	cf.OldestMultiDB = binary.LittleEndian.Uint32(data[96:100])

	// 4 bytes padding, then CheckPoint.time at offset 104
	cpTime := int64(binary.LittleEndian.Uint64(data[104:112]))
	cf.CheckpointTime = pgEpochToTime(cpTime)

	// CheckPoint.oldestActiveXid: TransactionId at offset 112
	cf.OldestActiveXID = binary.LittleEndian.Uint32(data[112:116])

	// CheckPoint.oldestCommitTsXid: TransactionId at offset 116
	cf.OldestCommitTsXID = binary.LittleEndian.Uint32(data[116:120])

	// CheckPoint.newestCommitTsXid: TransactionId at offset 120
	cf.NewestCommitTsXID = binary.LittleEndian.Uint32(data[120:124])

	// After CheckPoint structure, more fields follow
	// The exact offsets depend on version, but we can search for known patterns

	// Configuration parameters section starts around offset 200-220
	// Look for the wal_level and other settings

	// Find the configuration section by looking for max_connections pattern
	// These are typically small positive integers in sequence
	configOffset := findConfigSection(data, 180)
	if configOffset > 0 {
		// max_connections at configOffset
		cf.MaxConnections = int32(binary.LittleEndian.Uint32(data[configOffset : configOffset+4]))
		cf.MaxWorkerProcesses = int32(binary.LittleEndian.Uint32(data[configOffset+4 : configOffset+8]))
		cf.MaxWALSenders = int32(binary.LittleEndian.Uint32(data[configOffset+8 : configOffset+12]))
		cf.MaxPreparedXacts = int32(binary.LittleEndian.Uint32(data[configOffset+12 : configOffset+16]))
		cf.MaxLocksPerXact = int32(binary.LittleEndian.Uint32(data[configOffset+16 : configOffset+20]))

		// wal_level is before max_connections
		walLevel := int(binary.LittleEndian.Uint32(data[configOffset-8 : configOffset-4]))
		if walLevel >= 0 && walLevel < len(walLevelNames) {
			cf.WALLevel = walLevelNames[walLevel]
		}
		cf.WALLogHints = data[configOffset-4] != 0
		cf.TrackCommitTS = data[configOffset+20] != 0
	}

	// Storage parameters - find by looking for block_size (8192)
	storageOffset := findStorageSection(data, 220)
	if storageOffset > 0 {
		cf.MaxAlign = binary.LittleEndian.Uint32(data[storageOffset : storageOffset+4])
		cf.BlockSize = binary.LittleEndian.Uint32(data[storageOffset+8 : storageOffset+12])
		cf.BlocksPerSeg = binary.LittleEndian.Uint32(data[storageOffset+12 : storageOffset+16])
		cf.WALBlockSize = binary.LittleEndian.Uint32(data[storageOffset+16 : storageOffset+20])
		cf.WALSegmentSize = binary.LittleEndian.Uint32(data[storageOffset+20 : storageOffset+24])
		cf.NameDataLen = binary.LittleEndian.Uint32(data[storageOffset+24 : storageOffset+28])
		cf.IndexMaxKeys = binary.LittleEndian.Uint32(data[storageOffset+28 : storageOffset+32])
		cf.TOASTMaxChunk = binary.LittleEndian.Uint32(data[storageOffset+32 : storageOffset+36])
		cf.LargeObjectChunk = binary.LittleEndian.Uint32(data[storageOffset+36 : storageOffset+40])

		// Float format check (1234567.0 as float64)
		floatVal := math.Float64frombits(binary.LittleEndian.Uint64(data[storageOffset+40 : storageOffset+48]))
		cf.FloatFormatOK = floatVal == 1234567.0

		// Data checksums flag
		cf.DataChecksumsEnabled = data[storageOffset+48] != 0
	}

	// Default values if not found
	if cf.BlockSize == 0 {
		cf.BlockSize = 8192
	}
	if cf.WALBlockSize == 0 {
		cf.WALBlockSize = 8192
	}
	if cf.WALSegmentSize == 0 {
		cf.WALSegmentSize = 16 * 1024 * 1024
	}

	// CRC is at the very end of the control file (last 4 bytes before padding)
	// pg_control is typically 296 bytes but padded to 8KB
	// The actual CRC position depends on the structure size
	crcOffset := 288 // Typical position for PG12+
	if len(data) > crcOffset+4 {
		cf.CRC = binary.LittleEndian.Uint32(data[crcOffset : crcOffset+4])
		cf.CRCValid = verifyCRC32C(data[:crcOffset], cf.CRC)
	}

	return cf, nil
}

// findConfigSection finds the configuration parameters section
func findConfigSection(data []byte, startOffset int) int {
	// Look for max_connections pattern (typically 100)
	// followed by max_worker_processes (typically 8)
	for i := startOffset; i < len(data)-24 && i < 280; i += 4 {
		val1 := int32(binary.LittleEndian.Uint32(data[i : i+4]))
		val2 := int32(binary.LittleEndian.Uint32(data[i+4 : i+8]))

		// max_connections is usually 100, max_worker_processes is usually 8
		if val1 >= 1 && val1 <= 10000 && val2 >= 1 && val2 <= 1000 {
			// Verify by checking max_wal_senders (typically 10)
			val3 := int32(binary.LittleEndian.Uint32(data[i+8 : i+12]))
			if val3 >= 0 && val3 <= 1000 {
				return i
			}
		}
	}
	return 0
}

// findStorageSection finds the storage parameters section
func findStorageSection(data []byte, startOffset int) int {
	// Look for block_size (8192) pattern
	for i := startOffset; i < len(data)-48 && i < 300; i += 4 {
		// max_align is typically 8
		val0 := binary.LittleEndian.Uint32(data[i : i+4])
		// block_size is 8192
		val1 := binary.LittleEndian.Uint32(data[i+8 : i+12])
		// wal_block_size is 8192
		val3 := binary.LittleEndian.Uint32(data[i+16 : i+20])

		if val0 == 8 && val1 == 8192 && val3 == 8192 {
			return i
		}
	}
	return 0
}

// formatLSN formats an LSN as PostgreSQL does (high/low)
func formatLSN(lsn uint64) string {
	high := uint32(lsn >> 32)
	low := uint32(lsn & 0xFFFFFFFF)
	return fmt.Sprintf("%X/%X", high, low)
}

// formatWALFilename formats the WAL filename for a given LSN
func formatWALFilename(lsn uint64, timeline uint32) string {
	segSize := uint64(16 * 1024 * 1024) // 16MB default
	segNo := lsn / segSize
	return fmt.Sprintf("%08X%08X%08X", timeline, uint32(segNo>>32), uint32(segNo))
}

// pgTimeToGoTime converts PostgreSQL pg_time_t to Go time
// pg_time_t in pg_control is stored as Unix epoch (seconds since 1970-01-01)
func pgEpochToTime(pgTime int64) time.Time {
	return time.Unix(pgTime, 0).UTC()
}

// inferPGVersion attempts to determine PostgreSQL major version
func inferPGVersion(controlVersion, catalogVersion uint32) int {
	switch {
	case controlVersion >= 1300:
		if catalogVersion >= 202307071 {
			return 16
		}
		return 15
	case controlVersion >= 1201:
		if catalogVersion >= 202107181 {
			return 14
		}
		return 13
	case controlVersion >= 1100:
		if catalogVersion >= 201909212 {
			return 12
		}
		return 11
	case controlVersion >= 1002:
		return 10
	case controlVersion >= 960:
		return 9
	default:
		return 9
	}
}

// verifyCRC32C verifies CRC-32C checksum
func verifyCRC32C(data []byte, expected uint32) bool {
	crc := uint32(0xFFFFFFFF)
	table := makeCRC32CTable()

	for _, b := range data {
		crc = table[(crc^uint32(b))&0xFF] ^ (crc >> 8)
	}

	crc ^= 0xFFFFFFFF
	return crc == expected
}

// makeCRC32CTable generates CRC-32C lookup table
func makeCRC32CTable() [256]uint32 {
	var table [256]uint32
	const polynomial uint32 = 0x82F63B78

	for i := uint32(0); i < 256; i++ {
		crc := i
		for j := 0; j < 8; j++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ polynomial
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	return table
}

// ReadControlFile reads and parses pg_control from data directory
func ReadControlFile(dataDir string) (*ControlFile, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, "global", "pg_control"))
	if err != nil {
		return nil, err
	}
	return ParseControlFile(data)
}
