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
		detectPaths, listDBs, debug, sqlOutput     bool
	)

	flag.StringVar(&dataDir, "d", "", "PostgreSQL data directory (auto-detected if not set)")
	flag.StringVar(&singleFile, "f", "", "Single heap file to parse")
	flag.StringVar(&dbFilter, "db", "", "Filter by database name")
	flag.StringVar(&tableFilter, "t", "", "Filter tables containing string")
	flag.BoolVar(&listOnly, "list", false, "List schema only, no data")
	flag.BoolVar(&listDBs, "list-db", false, "List databases only")
	flag.BoolVar(&detectPaths, "detect", false, "Show detected PostgreSQL paths")
	flag.BoolVar(&sqlOutput, "sql", false, "Output as SQL statements (CREATE TABLE + INSERT)")
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

	if sqlOutput {
		if err := result.ToSQL(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating SQL: %v\n", err)
			os.Exit(1)
		}
	} else {
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
  pgread -sql -db mydb > backup.sql          Export database to SQL file
  pgread -detect                             Show detected PostgreSQL paths
  pgread -list-db                            List databases
  pgread -db mydb                            Dump specific database
  pgread -db mydb -t password                Filter tables
  pgread -d /path/to/data/                   Use specific data directory
  pgread -f /path/to/1262                    Parse single file

Fixed OIDs:
  1262  pg_database  (global/1262)
  1259  pg_class     (base/<oid>/1259)
  1249  pg_attribute (base/<oid>/1249)

Options:
`)
	flag.PrintDefaults()
}
