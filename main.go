package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Chocapikk/pgread/pgdump"
)


func main() {
	var (
		dataDir, singleFile, dbFilter, tableFilter string
		listOnly, verbose, showVersion             bool
		detectPaths, listDBs, debug                bool
		sqlOutput, csvOutput, tableOutput           bool
		searchPattern, passwords, secrets          string
		showDeleted, showWAL                       bool
		showControl, verifyChecksums               bool
		parseIndex, showDropped                    bool
		showSequences, showRelmap, blockRange      string
		binaryDump, skipOldValues, toastVerbose    bool
		segmentNumber, segmentSize                 int
	)

	flag.StringVar(&dataDir, "d", "", "PostgreSQL data directory (auto-detected if not set)")
	flag.StringVar(&singleFile, "f", "", "Single heap file to parse")
	flag.StringVar(&dbFilter, "db", "", "Filter by database name")
	flag.StringVar(&tableFilter, "t", "", "Filter tables containing string")
	flag.BoolVar(&listOnly, "list", false, "List schema only, no data")
	flag.BoolVar(&listDBs, "list-db", false, "List databases only")
	flag.BoolVar(&detectPaths, "detect", false, "Show detected PostgreSQL paths")
	flag.BoolVar(&sqlOutput, "sql", false, "Output as SQL statements")
	flag.BoolVar(&csvOutput, "csv", false, "Output as CSV")
	flag.BoolVar(&tableOutput, "table", false, "Output as formatted table (psql-style)")
	flag.StringVar(&searchPattern, "search", "", "Search for pattern in all tables (regex)")
	flag.StringVar(&passwords, "passwords", "", "Extract password hashes (use 'all' or specify user)")
	flag.StringVar(&secrets, "secrets", "", "Search for secrets/credentials (use 'auto' for common patterns)")
	flag.BoolVar(&showDeleted, "deleted", false, "Include deleted (non-vacuumed) rows")
	flag.BoolVar(&showWAL, "wal", false, "Show WAL (Write-Ahead Log) summary")
	flag.BoolVar(&showControl, "control", false, "Show pg_control file information")
	flag.BoolVar(&verifyChecksums, "checksum", false, "Verify page checksums")
	flag.BoolVar(&parseIndex, "index", false, "Parse index file (use with -f)")
	flag.BoolVar(&showDropped, "dropped", false, "Show dropped columns")
	flag.StringVar(&showSequences, "sequences", "", "Show sequences ('all' or database name)")
	flag.StringVar(&showRelmap, "relmap", "", "Show pg_filenode.map ('global', 'all', or db OID)")
	flag.StringVar(&blockRange, "R", "", "Block range to read (e.g., '0:10', '5:', ':20', '5')")
	flag.BoolVar(&binaryDump, "b", false, "Binary block dump (hex output)")
	flag.BoolVar(&skipOldValues, "o", false, "Skip old/dead tuple values")
	flag.BoolVar(&toastVerbose, "toast-verbose", false, "Verbose TOAST information")
	flag.IntVar(&segmentNumber, "n", 0, "Force segment number (for multi-segment files)")
	flag.IntVar(&segmentSize, "s", 0, "Force segment size in bytes (default: 1GB)")
	flag.BoolVar(&verbose, "v", false, "Verbose output")
	flag.BoolVar(&debug, "debug", false, "Debug tuple decoding")
	flag.BoolVar(&showVersion, "version", false, "Show version")
	flag.Usage = usage
	flag.Parse()

	pgdump.Debug = debug
	pgdump.DebugTable = tableFilter

	if showVersion {
		fmt.Printf("pgdump-offline %s\n", pgdump.Version)
		return
	}

	if detectPaths {
		paths := pgdump.DetectAllDataDirs()
		if len(paths) == 0 {
			fmt.Println("No PostgreSQL data directories found")
			os.Exit(1)
		}
		fmt.Println("Detected PostgreSQL data directories:")
		for _, p := range paths {
			dbs := pgdump.ListDatabases(p)
			dbNames := make([]string, 0, len(dbs))
			for _, db := range dbs {
				dbNames = append(dbNames, db.Name)
			}
			fmt.Printf("  %s (%s)\n", p, strings.Join(dbNames, ", "))
		}
		return
	}

	if singleFile != "" {
		// Build segment options if specified
		var segOpts *pgdump.SegmentOptions
		if segmentNumber > 0 || segmentSize > 0 {
			segOpts = &pgdump.SegmentOptions{
				SegmentNumber: segmentNumber,
				SegmentSize:   segmentSize,
			}
		}
		
		if binaryDump {
			parseBinaryDump(singleFile, blockRange)
		} else if parseIndex {
			parseIndexFile(singleFile)
		} else if toastVerbose {
			parseToastVerbose(singleFile)
		} else if blockRange != "" {
			parseBlockRangeWithSegment(singleFile, blockRange, segOpts)
		} else {
			parseSingle(singleFile)
		}
		return
	}

	// Auto-detect if no path provided
	if dataDir == "" {
		dataDir = pgdump.DetectDataDir()
		if dataDir == "" {
			fmt.Fprintln(os.Stderr, "Error: PostgreSQL data directory not found")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "Specify path manually:")
			fmt.Fprintln(os.Stderr, "  pgdump-offline -d /var/lib/postgresql/data/")
			fmt.Fprintln(os.Stderr, "")
			fmt.Fprintln(os.Stderr, "Or set PGDATA environment variable")
			os.Exit(1)
		}
		if verbose {
			fmt.Fprintf(os.Stderr, "[*] Auto-detected: %s\n", dataDir)
		}
	}

	// List databases only
	if listDBs {
		dbs := pgdump.ListDatabases(dataDir)
		if len(dbs) == 0 {
			fmt.Println("No databases found")
			os.Exit(1)
		}
		for _, db := range dbs {
			fmt.Printf("%s (OID %d)\n", db.Name, db.OID)
		}
		return
	}

	// Show pg_control information
	if showControl {
		cf, err := pgdump.ReadControlFile(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading pg_control: %v\n", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(cf)
		return
	}

	// Verify checksums
	if verifyChecksums {
		if verbose {
			fmt.Fprintln(os.Stderr, "[*] Verifying page checksums...")
		}
		result, err := pgdump.VerifyDataDirChecksums(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error verifying checksums: %v\n", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
		if result.InvalidBlocks > 0 {
			os.Exit(1)
		}
		return
	}

	// Show dropped columns
	if showDropped {
		if dbFilter != "" {
			result, err := pgdump.FindDroppedColumns(dataDir, dbFilter)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			enc.Encode(result)
		} else {
			results, err := pgdump.ScanDroppedColumns(dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			enc.Encode(results)
		}
		return
	}

	// Show sequences
	if showSequences != "" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if showSequences == "all" {
			results, err := pgdump.ScanAllSequences(dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc.Encode(results)
		} else {
			results, err := pgdump.FindSequences(dataDir, showSequences)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc.Encode(results)
		}
		return
	}

	// Show relmap
	if showRelmap != "" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if showRelmap == "global" {
			rm, err := pgdump.ReadGlobalRelMap(dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc.Encode(rm)
		} else if showRelmap == "all" {
			info, err := pgdump.ReadAllRelMaps(dataDir)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc.Encode(info)
		} else {
			// Assume it's a database OID
			oid, err := strconv.ParseUint(showRelmap, 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Invalid relmap option: %s (use 'global', 'all', or database OID)\n", showRelmap)
				os.Exit(1)
			}
			rm, err := pgdump.ReadDatabaseRelMap(dataDir, uint32(oid))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			enc.Encode(rm)
		}
		return
	}

	// Extract passwords
	if passwords != "" {
		auths, err := pgdump.ExtractPasswords(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error extracting passwords: %v\n", err)
			os.Exit(1)
		}
		if len(auths) == 0 {
			fmt.Println("No password hashes found")
			return
		}
		fmt.Println("PostgreSQL Password Hashes:")
		fmt.Println("===========================")
		for _, auth := range auths {
			if passwords != "all" && auth.RoleName != passwords {
				continue
			}
			flags := ""
			if auth.RolSuper {
				flags += " [SUPERUSER]"
			}
			if auth.RolLogin {
				flags += " [LOGIN]"
			}
			if auth.Password != "" {
				fmt.Printf("%s:%s%s\n", auth.RoleName, auth.Password, flags)
			} else {
				fmt.Printf("%s:(no password)%s\n", auth.RoleName, flags)
			}
		}
		return
	}

	// Search for secrets using trufflehog detectors
	if secrets != "" {
		if verbose {
			fmt.Fprintln(os.Stderr, "[*] Scanning for secrets with trufflehog detectors...")
		}
		findings, err := pgdump.ScanForSecrets(dataDir, &pgdump.Options{
			DatabaseFilter:   dbFilter,
			TableFilter:      tableFilter,
			SkipSystemTables: true,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if len(findings) == 0 {
			fmt.Println("No secrets found")
			return
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(findings)
		return
	}

	// Search mode
	if searchPattern != "" {
		results, err := pgdump.Search(dataDir, &pgdump.SearchOptions{
			Pattern:    searchPattern,
			IncludeRow: true,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(results)
		return
	}

	// WAL summary
	if showWAL {
		summary, err := pgdump.ScanWALDirectory(dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading WAL: %v\n", err)
			os.Exit(1)
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(summary)
		return
	}

	pgdump.Debug = debug
	result, err := pgdump.DumpDataDir(dataDir, &pgdump.Options{
		DatabaseFilter:   dbFilter,
		TableFilter:      tableFilter,
		ListOnly:         listOnly,
		SkipSystemTables: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		for _, db := range result.Databases {
			fmt.Fprintf(os.Stderr, "[*] %s (OID %d): %d tables\n", db.Name, db.OID, len(db.Tables))
		}
	}

	// Output format
	switch {
	case sqlOutput:
		if err := result.ToSQL(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating SQL: %v\n", err)
			os.Exit(1)
		}
	case csvOutput:
		if err := result.ToCSV(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating CSV: %v\n", err)
			os.Exit(1)
		}
	case tableOutput:
		result.TableFormat(os.Stdout)
	default:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
	}
}

func parseToastVerbose(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	
	// Try to detect TOAST table OID from path
	var toastOID uint32
	base := filepath.Base(path)
	if oid, err := strconv.ParseUint(base, 10, 32); err == nil {
		toastOID = uint32(oid)
	}
	
	info := pgdump.GetTOASTVerboseInfo(toastOID, data)
	if info == nil {
		fmt.Fprintln(os.Stderr, "No TOAST data found or not a TOAST table")
		os.Exit(1)
	}
	
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(info)
}

func parseBlockRangeWithSegment(path, rangeStr string, segOpts *pgdump.SegmentOptions) {
	br, err := pgdump.ParseBlockRange(rangeStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing block range: %v\n", err)
		os.Exit(1)
	}
	
	// If segment options provided, show segment info
	if segOpts != nil {
		segInfo, err := pgdump.GetSegmentInfo(path, segOpts)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error getting segment info: %v\n", err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "[*] Segment %d: %d blocks, global offset 0x%X\n", 
			segInfo.SegmentNumber, segInfo.TotalBlocks, segInfo.GlobalOffset)
	}
	
	blocks, err := pgdump.DumpBlockRange(path, br)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(blocks)
}

func parseBinaryDump(path, rangeStr string) {
	var br *pgdump.BlockRange
	var err error
	
	if rangeStr != "" {
		br, err = pgdump.ParseBlockRange(rangeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing block range: %v\n", err)
			os.Exit(1)
		}
	}
	
	dumps, err := pgdump.DumpBinaryRange(path, br)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	
	// Output as text hex dump (like xxd/hexdump)
	for _, d := range dumps {
		fmt.Printf("Block %d (offset 0x%08X):\n", d.BlockNumber, d.Offset)
		fmt.Println(d.HexDump)
	}
}

func parseIndexFile(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	
	info, err := pgdump.ParseIndexFile(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing index: %v\n", err)
		os.Exit(1)
	}
	
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(info)
}

func parseSingle(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	switch filepath.Base(path) {
	case "1262":
		fmt.Println("pg_database:")
		for _, db := range pgdump.ParsePGDatabase(data) {
			fmt.Printf("  %s (OID %d)\n", db.Name, db.OID)
		}
	case "1259":
		fmt.Println("pg_class:")
		for _, t := range pgdump.ParsePGClass(data) {
			fmt.Printf("  %s (OID %d, filenode %d, kind %s)\n", t.Name, t.OID, t.Filenode, t.Kind)
		}
	case "1249":
		fmt.Println("pg_attribute:")
		for relid, cols := range pgdump.ParsePGAttribute(data, 0) {
			fmt.Printf("  relation %d:\n", relid)
			for _, c := range cols {
				fmt.Printf("    %d: %s (%s)\n", c.Num, c.Name, pgdump.TypeName(c.TypID))
			}
		}
	default:
		tuples := pgdump.ParseFile(data)
		fmt.Printf("Heap file: %d tuples\n", len(tuples))
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `pgread - Dump PostgreSQL without credentials

Usage:
  pgread                                     Auto-detect and dump all (JSON)
  pgread -sql                                Output as SQL statements
  pgread -csv                                Output as CSV
  pgread -sql -db mydb > backup.sql          Export database to SQL file
  pgread -detect                             Show detected PostgreSQL paths
  pgread -list-db                            List databases
  pgread -db mydb                            Dump specific database
  pgread -db mydb -t password                Filter tables
  pgread -d /path/to/data/                   Use specific data directory
  pgread -f /path/to/1262                    Parse single file

Security / Forensics:
  pgread -passwords all                      Extract all password hashes
  pgread -passwords postgres                 Extract specific user's hash
  pgread -secrets auto                       Search for secrets (700+ patterns via Trufflehog)
  pgread -search "password|secret"           Search with custom regex
  pgread -deleted                            Include deleted (non-vacuumed) rows
  pgread -wal                                Show WAL transaction summary

Low-Level / Forensics:
  pgread -control                            Show pg_control file (version, state, LSN)
  pgread -checksum                           Verify page checksums (detect corruption)
  pgread -dropped                            Show dropped columns (recoverable data)
  pgread -sequences all                      Show all sequences
  pgread -sequences mydb                     Show sequences for specific database
  pgread -relmap global                      Show global pg_filenode.map
  pgread -relmap all                         Show all relmap files
  pgread -f /path/to/file -R 0:10            Read specific block range
  pgread -f /path/to/file -b                 Binary block dump (hex output)
  pgread -f /path/to/file -b -R 0:5          Binary dump of block range
  pgread -f /path/to/toast -toast-verbose    Verbose TOAST table info
  pgread -f /path/to/file -n 2 -R 0:10       Read from segment 2
  pgread -f /path/to/file -s 134217728       Custom segment size (128MB)
  pgread -f /path/to/index -index            Parse index file (BTree/GIN/GiST/Hash)

Fixed OIDs:
  1262  pg_database  (global/1262)
  1260  pg_authid    (global/1260) - passwords
  1259  pg_class     (base/<oid>/1259)
  1249  pg_attribute (base/<oid>/1249)

Options:
`)
	flag.PrintDefaults()
}
