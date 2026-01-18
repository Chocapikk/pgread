package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Chocapikk/pgdump-offline/pgdump"
)


func main() {
	var (
		dataDir, singleFile, dbFilter, tableFilter string
		listOnly, verbose, showVersion             bool
	)

	flag.StringVar(&dataDir, "d", "", "PostgreSQL data directory")
	flag.StringVar(&singleFile, "f", "", "Single heap file to parse")
	flag.StringVar(&dbFilter, "db", "", "Filter by database name")
	flag.StringVar(&tableFilter, "t", "", "Filter tables containing string")
	flag.BoolVar(&listOnly, "list", false, "List schema only, no data")
	flag.BoolVar(&verbose, "v", false, "Verbose output")
	flag.BoolVar(&showVersion, "version", false, "Show version")
	flag.Usage = usage
	flag.Parse()

	if showVersion {
		fmt.Printf("pgdump-offline %s\n", pgdump.Version)
		return
	}

	if singleFile != "" {
		parseSingle(singleFile)
		return
	}

	if dataDir == "" {
		fmt.Fprintln(os.Stderr, "Error: -d or -f required")
		flag.Usage()
		os.Exit(1)
	}

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

	json.NewEncoder(os.Stdout).SetIndent("", "  ")
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(result)
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
	fmt.Fprintf(os.Stderr, `pgdump-offline - Dump PostgreSQL from leaked heap files

Usage:
  pgdump-offline -d /path/to/data/           Dump all databases
  pgdump-offline -d /path/to/data/ -db mydb  Dump specific database  
  pgdump-offline -d /path/to/data/ -t pass   Filter tables
  pgdump-offline -f /path/to/1262            Parse single file

Fixed OIDs:
  1262  pg_database  (global/1262)
  1259  pg_class     (base/<oid>/1259)
  1249  pg_attribute (base/<oid>/1249)

Options:
`)
	flag.PrintDefaults()
}
