package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Chocapikk/pgread/pgdump"
)


func main() {
	var (
		dataDir, singleFile, dbFilter, tableFilter string
		listOnly, verbose, showVersion             bool
		detectPaths, listDBs, debug                bool
		sqlOutput, csvOutput                       bool
		searchPattern, passwords, secrets          string
		showDeleted, showWAL                       bool
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
	flag.StringVar(&searchPattern, "search", "", "Search for pattern in all tables (regex)")
	flag.StringVar(&passwords, "passwords", "", "Extract password hashes (use 'all' or specify user)")
	flag.StringVar(&secrets, "secrets", "", "Search for secrets/credentials (use 'auto' for common patterns)")
	flag.BoolVar(&showDeleted, "deleted", false, "Include deleted (non-vacuumed) rows")
	flag.BoolVar(&showWAL, "wal", false, "Show WAL (Write-Ahead Log) summary")
	flag.BoolVar(&verbose, "v", false, "Verbose output")
	flag.BoolVar(&debug, "debug", false, "Debug tuple decoding")
	flag.BoolVar(&showVersion, "version", false, "Show version")
	flag.Usage = usage
	flag.Parse()

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
		parseSingle(singleFile)
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
	default:
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		enc.Encode(result)
	}
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
  pgread -secrets auto                       Search for common secrets (API keys, etc)
  pgread -search "password|secret"           Search with custom regex
  pgread -deleted                            Include deleted (non-vacuumed) rows
  pgread -wal                                Show WAL transaction summary

Fixed OIDs:
  1262  pg_database  (global/1262)
  1260  pg_authid    (global/1260) - passwords
  1259  pg_class     (base/<oid>/1259)
  1249  pg_attribute (base/<oid>/1249)

Options:
`)
	flag.PrintDefaults()
}
