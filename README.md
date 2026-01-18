# pgread

[![Test](https://github.com/Chocapikk/pgread/actions/workflows/test.yml/badge.svg)](https://github.com/Chocapikk/pgread/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/Chocapikk/pgread/branch/main/graph/badge.svg)](https://codecov.io/gh/Chocapikk/pgread)
[![Go Report Card](https://goreportcard.com/badge/github.com/Chocapikk/pgread)](https://goreportcard.com/report/github.com/Chocapikk/pgread)

Dump PostgreSQL data without credentials - if you can read the files, you can dump the database.

## The Technique

PostgreSQL uses **fixed OIDs** for system catalogs:

| OID | Catalog | Path |
|-----|---------|------|
| 1262 | pg_database | `global/1262` |
| 1259 | pg_class | `base/<db_oid>/1259` |
| 1249 | pg_attribute | `base/<db_oid>/1249` |

Leak these 3 files → discover entire schema → dump any table.

## Install

```bash
go install github.com/Chocapikk/pgread@latest
```

## CLI

```bash
# Basic usage
pgread                                # Auto-detect and dump (JSON)
pgread -sql                           # Output as SQL statements
pgread -csv                           # Output as CSV
pgread -sql -db mydb > backup.sql     # Export to SQL file
pgread -d /path/to/data/              # Specify data directory
pgread -d /path/to/data/ -db mydb     # Specific database
pgread -d /path/to/data/ -t password  # Filter tables
pgread -d /path/to/data/ -list        # Schema only
pgread -f /path/to/1262               # Parse single file

# Security / Forensics
pgread -passwords all                 # Extract ALL password hashes
pgread -passwords postgres            # Extract specific user's hash
pgread -secrets auto                  # Auto-detect secrets (API keys, etc)
pgread -search "password|secret"      # Search with regex
pgread -deleted                       # Include deleted rows (forensics)
pgread -wal                           # WAL transaction summary
pgread -detect                        # Show detected PostgreSQL paths

# Low-Level / Forensics
pgread -control                       # pg_control file (version, state, LSN)
pgread -checksum                      # Verify page checksums (corruption)
pgread -dropped                       # Show dropped columns (recoverable)
pgread -dropped -db mydb              # Dropped columns for specific DB
pgread -f /path/to/index -index       # Parse index file (BTree/GIN/GiST/Hash)
```

### Password Extraction

```bash
$ pgread -passwords all
PostgreSQL Password Hashes:
===========================
postgres:SCRAM-SHA-256$4096:salt$hash:proof [SUPERUSER] [LOGIN]
admin:SCRAM-SHA-256$4096:salt$hash:proof [LOGIN]
```

### Secret Detection (Powered by Trufflehog)

Uses [trufflehog](https://github.com/trufflesecurity/trufflehog)'s 700+ detectors:

```bash
$ pgread -secrets auto
[
  {
    "detector": "Stripe",
    "database": "postgres",
    "table": "api_keys",
    "column": "value",
    "raw": "sk_live_51Hx...",
    "extra_data": {
      "rotation_guide": "https://howtorotate.com/docs/tutorials/stripe/"
    }
  }
]
```

Detects: Stripe, AWS, GitHub, GitLab, Slack, SendGrid, Doppler, DigitalOcean, Heroku, and 700+ more.

### WAL Analysis

```bash
$ pgread -wal
{
  "segment_count": 1,
  "record_count": 24574,
  "pg_version": "16",
  "operations": {
    "INSERT": 4440,
    "DELETE": 106,
    "UPDATE": 379,
    "COMMIT": 738,
    ...
  },
  "transactions": [...]
}
```

### pg_control Parsing

```bash
$ pgread -control
{
  "pg_control_version": 1300,
  "catalog_version_no": 202307071,
  "system_identifier": 7123456789012345678,
  "state": 6,
  "state_string": "IN_PRODUCTION",
  "checkpoint_lsn": 4294967376,
  "checkpoint_lsn_str": "0/100000050",
  "pg_version_major": 16,
  "data_checksums_enabled": true,
  ...
}
```

### Checksum Verification

```bash
$ pgread -checksum
{
  "data_dir": "/var/lib/postgresql/data",
  "checksums_enabled": true,
  "total_files": 42,
  "total_blocks": 1024,
  "valid_blocks": 1024,
  "invalid_blocks": 0
}
```

Detects page corruption before PostgreSQL does!

### Index Parsing

```bash
$ pgread -f /var/lib/postgresql/data/base/16384/16385 -index
{
  "type": 1,
  "type_string": "btree",
  "total_pages": 5,
  "root_page": 1,
  "levels": 2,
  "meta": {
    "magic": 340322,
    "version": 4,
    "root": 1,
    "level": 2
  },
  "pages": [...]
}
```

Supports: **BTree**, **GIN**, **GiST**, **Hash**, **SP-GiST**

### Dropped Columns Recovery

```bash
$ pgread -dropped
[
  {
    "database": "mydb",
    "dropped_count": 2,
    "columns": [
      {
        "rel_oid": 16384,
        "table_name": "users",
        "attnum": 3,
        "dropped_name": "........pg.dropped.3........",
        "type_oid": 25,
        "type_name": "text"
      }
    ]
  }
]
```

Recover data from columns that were `ALTER TABLE DROP COLUMN`!

### SQL/CSV Export

```bash
# Export entire database
pgread -sql -db mydb > mydb_backup.sql
psql -d newdb < mydb_backup.sql

# Export to CSV
pgread -csv -db mydb > mydb.csv
```

## Library

```go
import "github.com/Chocapikk/pgread/pgdump"

// Auto-detect and dump ALL PostgreSQL instances
results, _ := pgdump.DumpAll(nil)

// Or specify a path
result, _ := pgdump.DumpDataDir("/var/lib/postgresql/data", nil)

// With options
result, _ := pgdump.DumpDataDir("/path/to/data", &pgdump.Options{
    DatabaseFilter: "mydb",
    TableFilter:    "password",
})

// Custom file reader (arbitrary file read, SSRF, backups, etc.)
pgdump.DumpDatabaseFromFiles(classData, attrData, func(fn uint32) ([]byte, error) {
    return httpClient.Get(fmt.Sprintf("/base/%d/%d", dbOID, fn))
}, nil)

// Export to SQL
result, _ := pgdump.DumpDataDir("/path/to/data", nil)
result.ToSQL(os.Stdout)  // or any io.Writer
```

### Auto-Detection

```go
// Find first PostgreSQL data directory
dataDir := pgdump.DetectDataDir()

// Find ALL PostgreSQL data directories
dataDirs := pgdump.DetectAllDataDirs()
```

### Low-Level API

```go
// Parse system catalogs
databases := pgdump.ParsePGDatabase(data)  // []DatabaseInfo
tables := pgdump.ParsePGClass(data)        // map[filenode]TableInfo
columns := pgdump.ParsePGAttribute(data,0) // map[oid][]AttrInfo

// Decode table data
rows := pgdump.ReadRows(tableData, schema, true)

// Raw tuple access
tuples := pgdump.ReadTuples(data, true)
row := pgdump.DecodeTuple(tuple, columns)

// pg_control parsing
control, _ := pgdump.ReadControlFile(dataDir)
fmt.Printf("PG Version: %d, State: %s\n", control.PGVersionMajor, control.StateString)

// Checksum verification
result, _ := pgdump.VerifyDataDirChecksums(dataDir)
fmt.Printf("Valid: %d, Invalid: %d\n", result.ValidBlocks, result.InvalidBlocks)

// Index parsing
indexInfo, _ := pgdump.ParseIndexFile(data)
fmt.Printf("Type: %s, Root: %d\n", indexInfo.TypeString, indexInfo.RootPage)

// Dropped columns
dropped, _ := pgdump.FindDroppedColumns(dataDir, "mydb")
for _, col := range dropped.Columns {
    fmt.Printf("Dropped: %s.%d (%s)\n", col.TableName, col.AttNum, col.TypeName)
}
```

## Supported Types

**Numeric:** `bool` `int2` `int4` `int8` `float4` `float8` `numeric` `money`

**Text:** `text` `varchar` `char` `bpchar` `name` `bytea`

**Date/Time:** `date` `time` `timetz` `timestamp` `timestamptz` `interval`

**Network:** `inet` `cidr` `macaddr` `macaddr8`

**Geometric:** `point` `line` `lseg` `box` `circle` `path` `polygon`

**Structured:** `json` `jsonb` `jsonpath` `xml` `uuid`

**Range:** `int4range` `int8range` `numrange` `daterange` `tsrange` `tstzrange`

**Text Search:** `tsvector` `tsquery`

**Other:** `oid` `tid` `xid` `cid` `pg_lsn` `bit` `varbit` + **arrays of all above**

## Build

```bash
go build
GOOS=windows go build -o pgread.exe
GOOS=darwin GOARCH=arm64 go build -o pgread-macos
```

## License

[WTFPL](http://www.wtfpl.net/) - Do What The Fuck You Want To Public License
