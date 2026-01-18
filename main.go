// pgdump-offline - Dump PostgreSQL data from leaked heap files
//
// Usage:
//
//	pgdump-offline -d /path/to/pg_data/         # Auto-discover and dump all
//	pgdump-offline -f /path/to/file             # Parse single heap file
//	pgdump-offline -d /path/to/pg_data/ -db windmill -t password  # Filter
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/chocapikk/pgdump-offline/pkg"
)

type Config struct {
	DataDir      string
	SingleFile   string
	DatabaseName string
	TableFilter  string
	OutputJSON   bool
	ListOnly     bool
	Verbose      bool
}

type DatabaseDump struct {
	Databases []DatabaseResult `json:"databases"`
}

type DatabaseResult struct {
	OID    uint32        `json:"oid"`
	Name   string        `json:"name"`
	Tables []TableResult `json:"tables"`
}

type TableResult struct {
	OID      uint32                   `json:"oid"`
	Name     string                   `json:"name"`
	Filenode uint32                   `json:"filenode"`
	Kind     string                   `json:"kind"`
	Columns  []ColumnResult           `json:"columns,omitempty"`
	Rows     []map[string]interface{} `json:"rows,omitempty"`
	RowCount int                      `json:"row_count"`
}

type ColumnResult struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	TypID int    `json:"typid"`
}

func main() {
	cfg := parseFlags()

	if cfg.SingleFile != "" {
		parseSingleFile(cfg)
		return
	}

	if cfg.DataDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -d (data directory) or -f (single file) required")
		flag.Usage()
		os.Exit(1)
	}

	result := dumpDataDirectory(cfg)

	if cfg.OutputJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
	}
}

func parseFlags() Config {
	cfg := Config{}

	flag.StringVar(&cfg.DataDir, "d", "", "PostgreSQL data directory (e.g., /var/lib/postgresql/data)")
	flag.StringVar(&cfg.SingleFile, "f", "", "Single heap file to parse")
	flag.StringVar(&cfg.DatabaseName, "db", "", "Filter by database name")
	flag.StringVar(&cfg.TableFilter, "t", "", "Filter tables containing this string")
	flag.BoolVar(&cfg.OutputJSON, "json", true, "Output as JSON")
	flag.BoolVar(&cfg.ListOnly, "list", false, "List databases/tables only, don't dump data")
	flag.BoolVar(&cfg.Verbose, "v", false, "Verbose output")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `pgdump-offline - Dump PostgreSQL data from leaked heap files

Usage:
  %s -d /path/to/pg_data/                    # Auto-discover and dump all
  %s -d /path/to/pg_data/ -list              # List databases and tables
  %s -d /path/to/pg_data/ -db windmill       # Dump specific database
  %s -d /path/to/pg_data/ -t password        # Dump tables matching filter
  %s -f /path/to/1259                        # Parse single file (pg_class)

Fixed OIDs (works on any PostgreSQL):
  1262 - pg_database (global/1262)
  1259 - pg_class    (base/<db_oid>/1259)  
  1249 - pg_attribute (base/<db_oid>/1249)

Options:
`, os.Args[0], os.Args[0], os.Args[0], os.Args[0], os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()
	return cfg
}

func parseSingleFile(cfg Config) {
	data, err := os.ReadFile(cfg.SingleFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	filename := filepath.Base(cfg.SingleFile)

	// Try to detect what kind of file this is
	switch filename {
	case "1262":
		fmt.Println("Detected: pg_database (global)")
		dbs := pkg.ParsePGDatabase(data)
		for _, db := range dbs {
			fmt.Printf("  Database: %s (OID: %d)\n", db.Name, db.OID)
		}

	case "1259":
		fmt.Println("Detected: pg_class")
		tables := pkg.ParsePGClass(data)
		for _, t := range tables {
			fmt.Printf("  Table: %s (OID: %d, filenode: %d, kind: %s)\n",
				t.Name, t.OID, t.Filenode, t.Kind)
		}

	case "1249":
		fmt.Println("Detected: pg_attribute")
		attrs := pkg.ParsePGAttribute(data, 0)
		for relid, cols := range attrs {
			fmt.Printf("  Relation %d:\n", relid)
			for _, c := range cols {
				fmt.Printf("    %d: %s (%s)\n", c.Num, c.Name, pkg.TypeName(c.TypID))
			}
		}

	default:
		fmt.Println("Generic heap file - extracting tuples")
		tuples := pkg.ReadTuples(data, true)
		fmt.Printf("Found %d tuples\n", len(tuples))

		// Try to dump as raw data
		for i, t := range tuples {
			if i >= 10 {
				fmt.Printf("... and %d more\n", len(tuples)-10)
				break
			}
			fmt.Printf("Tuple %d: %d bytes\n", i, len(t.Tuple.Data))
		}
	}
}

func dumpDataDirectory(cfg Config) *DatabaseDump {
	result := &DatabaseDump{}

	// Step 1: Read pg_database (global/1262)
	pgDatabasePath := filepath.Join(cfg.DataDir, "global", "1262")
	dbData, err := os.ReadFile(pgDatabasePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading pg_database: %v\n", err)
		fmt.Fprintf(os.Stderr, "Expected path: %s\n", pgDatabasePath)
		os.Exit(1)
	}

	databases := pkg.ParsePGDatabase(dbData)
	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "[*] Found %d databases\n", len(databases))
	}

	// Step 2: Process each database
	for _, db := range databases {
		// Skip template databases
		if strings.HasPrefix(db.Name, "template") {
			continue
		}

		// Filter by database name if specified
		if cfg.DatabaseName != "" && db.Name != cfg.DatabaseName {
			continue
		}

		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "[*] Processing database: %s (OID: %d)\n", db.Name, db.OID)
		}

		dbResult := processDatabaseWithSchemaDiscovery(cfg, db)
		if dbResult != nil {
			result.Databases = append(result.Databases, *dbResult)
		}
	}

	return result
}

func processDatabaseWithSchemaDiscovery(cfg Config, db pkg.DatabaseInfo) *DatabaseResult {
	basePath := filepath.Join(cfg.DataDir, "base", strconv.FormatUint(uint64(db.OID), 10))

	// Read pg_class (1259)
	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "  Warning: Cannot read pg_class: %v\n", err)
		}
		return nil
	}

	// Read pg_attribute (1249)
	attrData, err := os.ReadFile(filepath.Join(basePath, "1249"))
	if err != nil {
		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "  Warning: Cannot read pg_attribute: %v\n", err)
		}
		return nil
	}

	// Parse schema
	tables := pkg.ParsePGClass(classData)
	attributes := pkg.ParsePGAttribute(attrData, 0)

	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "  Found %d tables\n", len(tables))
	}

	result := &DatabaseResult{
		OID:  db.OID,
		Name: db.Name,
	}

	// Process each table
	for filenode, tableInfo := range tables {
		// Skip system tables (kind != 'r' for regular table)
		if tableInfo.Kind != "r" && tableInfo.Kind != "" {
			continue
		}

		// Skip pg_ tables (system catalogs)
		if strings.HasPrefix(tableInfo.Name, "pg_") {
			continue
		}

		// Apply table filter
		if cfg.TableFilter != "" && !strings.Contains(strings.ToLower(tableInfo.Name), strings.ToLower(cfg.TableFilter)) {
			continue
		}

		tableResult := processTable(cfg, basePath, filenode, tableInfo, attributes)
		if tableResult != nil {
			result.Tables = append(result.Tables, *tableResult)
		}
	}

	return result
}

func processTable(cfg Config, basePath string, filenode uint32, tableInfo pkg.TableInfo, attributes map[uint32][]pkg.AttrInfo) *TableResult {
	// Build column schema
	attrs := attributes[tableInfo.OID]

	result := &TableResult{
		OID:      tableInfo.OID,
		Name:     tableInfo.Name,
		Filenode: filenode,
		Kind:     tableInfo.Kind,
	}

	// Add column info
	for _, attr := range attrs {
		result.Columns = append(result.Columns, ColumnResult{
			Name:  attr.Name,
			Type:  pkg.TypeName(attr.TypID),
			TypID: attr.TypID,
		})
	}

	if cfg.ListOnly {
		return result
	}

	// Read table data
	tablePath := filepath.Join(basePath, strconv.FormatUint(uint64(filenode), 10))
	tableData, err := os.ReadFile(tablePath)
	if err != nil {
		if cfg.Verbose {
			fmt.Fprintf(os.Stderr, "    Warning: Cannot read table %s: %v\n", tableInfo.Name, err)
		}
		return result
	}

	// Convert AttrInfo to Column for decoding
	columns := make([]pkg.Column, len(attrs))
	for i, attr := range attrs {
		columns[i] = pkg.Column{
			Name:  attr.Name,
			TypID: attr.TypID,
			Len:   attr.Len,
			Num:   attr.Num,
		}
	}

	// Decode rows
	rows := pkg.ReadRows(tableData, columns, true)
	result.Rows = rows
	result.RowCount = len(rows)

	if cfg.Verbose {
		fmt.Fprintf(os.Stderr, "    Table %s: %d rows\n", tableInfo.Name, len(rows))
	}

	return result
}
