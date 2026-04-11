// Package pgdump parses PostgreSQL heap files without credentials.
//
// If you can read the files, you can dump the database.
// Uses fixed system catalog OIDs to auto-discover schema:
//   - 1262: pg_database (global/1262)
//   - 1259: pg_class (base/<db_oid>/1259)
//   - 1249: pg_attribute (base/<db_oid>/1249)
//
// # Quick Start (Auto-detect)
//
//	// Dump all PostgreSQL instances found on the system
//	results, _ := pgdump.DumpAll(nil)
//	for _, result := range results {
//	    for _, db := range result.Databases {
//	        for _, t := range db.Tables {
//	            fmt.Println(t.Name, t.Rows)
//	        }
//	    }
//	}
//
// # With Path
//
//	result, _ := pgdump.DumpDataDir("/var/lib/postgresql/data", nil)
//
// # With Options
//
//	result, _ := pgdump.DumpDataDir("/path/to/data", &pgdump.Options{
//	    DatabaseFilter: "mydb",
//	    TableFilter:    "password",
//	})
//
// # Custom File Reader (SSRF, arbitrary file read, backups)
//
//	pgdump.DumpDatabaseFromFiles(classData, attrData, func(fn uint32) ([]byte, error) {
//	    return httpClient.Get(fmt.Sprintf("/base/%d/%d", dbOID, fn))
//	}, nil)
package pgdump

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Version is set at build time via ldflags
var Version = "dev"

// Options configures dump behavior
type Options struct {
	DatabaseFilter   string // Filter by database name
	TableFilter      string // Filter tables containing string
	ListOnly         bool   // Schema only, no data
	SkipSystemTables bool   // Skip pg_* tables (default: true)
	PostgresVersion  int    // Hint PG version (0 = auto)
}

// DumpResult contains complete dump
type DumpResult struct {
	Databases []DatabaseDump `json:"databases"`
}

// DatabaseDump contains single database dump
type DatabaseDump struct {
	OID    uint32      `json:"oid"`
	Name   string      `json:"name"`
	Tables []TableDump `json:"tables"`
}

// TableDump contains single table dump
type TableDump struct {
	OID      uint32                   `json:"oid"`
	Name     string                   `json:"name"`
	Filenode uint32                   `json:"filenode"`
	Kind     string                   `json:"kind"`
	Columns  []ColumnInfo             `json:"columns,omitempty"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	RowCount int                      `json:"row_count"`
}

// ColumnInfo describes a column
type ColumnInfo struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	TypID int    `json:"typid"`
}

// FileReader reads table data by filenode
type FileReader func(filenode uint32) ([]byte, error)

// DumpAll auto-detects all PostgreSQL data directories and dumps them.
// Returns a slice of results, one per data directory found.
func DumpAll(opts *Options) ([]*DumpResult, error) {
	dirs := DetectAllDataDirs()
	if len(dirs) == 0 {
		return nil, nil
	}

	var results []*DumpResult
	for _, dir := range dirs {
		if result, err := DumpDataDir(dir, opts); err == nil && result != nil {
			results = append(results, result)
		}
	}
	return results, nil
}

// DumpDataDir dumps all databases from a data directory
func DumpDataDir(dataDir string, opts *Options) (*DumpResult, error) {
	opts = withDefaults(opts)

	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, err
	}

	result := &DumpResult{}
	for _, db := range ParsePGDatabase(dbData) {
		if isTemplateDB(db.Name) {
			continue
		}
		if opts.DatabaseFilter != "" && db.Name != opts.DatabaseFilter {
			continue
		}

		basePath := filepath.Join(dataDir, "base", strconv.FormatUint(uint64(db.OID), 10))
		classData, _ := os.ReadFile(filepath.Join(basePath, "1259"))
		attrData, _ := os.ReadFile(filepath.Join(basePath, "1249"))

		if len(classData) == 0 {
			continue
		}

		reader := func(fn uint32) ([]byte, error) {
			return os.ReadFile(filepath.Join(basePath, strconv.FormatUint(uint64(fn), 10)))
		}

		if dump, _ := DumpDatabaseFromFiles(classData, attrData, reader, opts); dump != nil {
			dump.OID, dump.Name = db.OID, db.Name
			result.Databases = append(result.Databases, *dump)
		}
	}
	return result, nil
}

// DumpDatabaseFromFiles dumps using pre-read catalog files and custom reader
func DumpDatabaseFromFiles(classData, attrData []byte, reader FileReader, opts *Options) (*DatabaseDump, error) {
	opts = withDefaults(opts)

	tables := ParsePGClass(classData)
	attrs := ParsePGAttribute(attrData, opts.PostgresVersion)

	// Build OID-to-filenode map for resolving TOAST table filenodes.
	// After VACUUM FULL, the OID (reltoastrelid) stays the same but the
	// relfilenode changes. The file on disk is named by filenode, not OID.
	oidToFilenode := make(map[uint32]uint32)
	for fn, ti := range tables {
		oidToFilenode[ti.OID] = fn
	}

	ctx := &dumpContext{
		reader: reader,
		opts:   opts,
		toastReader: &TOASTReader{
			chunks: make(map[uint32][]TOASTChunk),
		},
		oidToFilenode: oidToFilenode,
	}

	result := &DatabaseDump{}
	for filenode, info := range tables {
		if info.Kind != "r" && info.Kind != "" {
			continue
		}
		if opts.SkipSystemTables && strings.HasPrefix(info.Name, "pg_") {
			continue
		}
		if opts.TableFilter != "" && !strings.Contains(strings.ToLower(info.Name), strings.ToLower(opts.TableFilter)) {
			continue
		}

		table := dumpTable(filenode, info, attrs[info.OID], ctx)
		result.Tables = append(result.Tables, table)
	}
	return result, nil
}

// dumpContext holds shared state for dumping tables within a database
type dumpContext struct {
	reader        FileReader
	opts          *Options
	toastReader   *TOASTReader
	oidToFilenode map[uint32]uint32
}

func dumpTable(filenode uint32, info TableInfo, attrs []AttrInfo, ctx *dumpContext) TableDump {
	t := TableDump{
		OID:      info.OID,
		Name:     info.Name,
		Filenode: filenode,
		Kind:     info.Kind,
	}

	for _, a := range attrs {
		t.Columns = append(t.Columns, ColumnInfo{
			Name:  a.Name,
			Type:  TypeName(a.TypID),
			TypID: a.TypID,
		})
	}

	if ctx.opts.ListOnly || ctx.reader == nil {
		return t
	}

	data, err := ctx.reader(filenode)
	if err != nil || len(data) == 0 {
		return t
	}

	cols := make([]Column, len(attrs))
	for i, a := range attrs {
		cols[i] = Column{Name: a.Name, TypID: a.TypID, Len: a.Len, Num: a.Num, Align: a.Align}
	}

	// Load TOAST table if this table has one and a reader is available
	var tableToastReader *TOASTReader
	if info.ToastRelID != 0 && ctx.toastReader != nil {
		if _, loaded := ctx.toastReader.chunks[info.ToastRelID]; !loaded {
			toastFilenode := info.ToastRelID
			if fn, ok := ctx.oidToFilenode[info.ToastRelID]; ok {
				toastFilenode = fn
			}
			if toastData, err := ctx.reader(toastFilenode); err == nil && len(toastData) > 0 {
				ctx.toastReader.LoadTOASTTable(info.ToastRelID, toastData)
			}
		}
		tableToastReader = ctx.toastReader
	}

	t.Rows = ReadRowsWithTOAST(data, cols, true, tableToastReader)
	t.RowCount = len(t.Rows)
	return t
}

func withDefaults(opts *Options) *Options {
	if opts == nil {
		return &Options{SkipSystemTables: true}
	}
	return opts
}

// ParseFile parses single heap file returning raw tuples
func ParseFile(data []byte) []TupleEntry {
	return ReadTuples(data, true)
}
