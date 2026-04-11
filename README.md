# pgread

[![Test](https://github.com/Chocapikk/pgread/actions/workflows/test.yml/badge.svg)](https://github.com/Chocapikk/pgread/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/Chocapikk/pgread/branch/main/graph/badge.svg)](https://codecov.io/gh/Chocapikk/pgread)
[![Go Report Card](https://goreportcard.com/badge/github.com/Chocapikk/pgread)](https://goreportcard.com/report/github.com/Chocapikk/pgread)

Dump PostgreSQL data without credentials - if you can read the files, you can dump the database.

> **Blog post:** [Dumping PostgreSQL Without Credentials: Heap File Parsing for Offensive Security](https://chocapikk.com/posts/2026/dumping-postgresql-without-credentials/)

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
pgread -table                         # Output as psql-style table
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
pgread -sequences all                 # List all sequences with values
pgread -relmap global                 # Show pg_filenode.map (OID→filenode)
pgread -f /path/to/file -R 0:10       # Read specific block range
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

### Sequence Parsing

```bash
$ pgread -sequences mydb
[
  {
    "name": "users_id_seq",
    "oid": 16396,
    "filenode": 16396,
    "last_value": 42,
    "start_value": 1,
    "increment_by": 1,
    "max_value": 9223372036854775807,
    "min_value": 1,
    "is_cycled": false,
    "is_called": true
  }
]
```

### pg_filenode.map Parsing

```bash
$ pgread -relmap global
{
  "magic": 5842711,
  "num_mappings": 50,
  "mappings": [
    {"oid": 1262, "filenode": 1262},
    {"oid": 1260, "filenode": 1260},
    ...
  ]
}
```

Maps system catalog OIDs to their physical filenodes.

### Block Range Selection

```bash
$ pgread -f /path/to/heap -R 0:5
[
  {
    "block_number": 0,
    "lsn": "0/19921E0",
    "checksum": 0,
    "lower": 212,
    "upper": 7744,
    "page_size": 8192,
    "item_count": 47,
    "free_space": 7532
  },
  ...
]
```

Read specific blocks: `0:10` (blocks 0-10), `5:` (from 5), `:20` (up to 20), `5` (block 5 only).

### Binary Block Dump

```bash
$ pgread -f /path/to/heap -b -R 0
Block 0 (offset 0x00000000):
00000000  00 00 00 00 40 2e 4f 01  00 00 01 00 30 00 20 1d  |....@.O.....0. .|
00000010  00 20 04 20 00 00 00 00  05 00 01 00 06 00 01 00  |. . ............|
...
```

Raw hex dump like `xxd` or `hexdump -C`. Useful for low-level forensics.

### Multi-Segment Files

PostgreSQL splits large tables into 1GB segments. pgread handles this:

```bash
# Read from specific segment
$ pgread -f /path/to/16384.2 -n 2 -R 0:10

# Custom segment size (e.g., 128MB for some configs)
$ pgread -f /path/to/file -s 134217728 -R 0:100
```

### TOAST Verbose

```bash
$ pgread -f /path/to/toast_table -toast-verbose
{
  "toast_rel_id": 16385,
  "total_chunks": 150,
  "unique_values": 42,
  "total_size": 1048576,
  "average_chunk_size": 6990.5,
  "max_chunks_per_value": 12,
  "chunk_distribution": {"1": 20, "2": 15, "5": 5, "12": 2}
}
```

### Table Output (psql-style)

```bash
$ pgread -d /path/to/data -db mydb -t users -table

 mydb.users (3 rows)
 email              | password_hash                                 | is_admin
--------------------+-----------------------------------------------+---------
 admin@example.com  | $argon2id$v=19$m=19456,t=2,p=1$salt$hash      | true
 alice@example.com  | $argon2id$v=19$m=19456,t=2,p=1$salt$hash      | false
 bob@example.com    | $argon2id$v=19$m=19456,t=2,p=1$salt$hash      | false
(3 rows)
```

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

## Known Limitations

- **TOAST & Compression:** Large values stored in TOAST tables are automatically resolved, including after `VACUUM FULL`. Supports **PGLZ** and **LZ4** compression, both inline (small compressed values kept in the main heap) and external (chunked in TOAST tables). Works with PostgreSQL 12-17.
- **Encrypted data:** Application-level encryption is returned as-is (ciphertext). pgread extracts what PostgreSQL stores.
- **In-flight data:** Recently written data still in shared buffers may not be on disk yet. Run `CHECKPOINT` first if possible.

## Related

- [Windfall](https://github.com/Chocapikk/Windfall) - Exploit toolkit using this technique for Windmill/Nextcloud Flow RCE
- [Blog post: Dumping PostgreSQL Without Credentials](https://chocapikk.com/posts/2026/dumping-postgresql-without-credentials/)
- [Blog post: Windfall - From Path Traversal to RCE](https://chocapikk.com/posts/2026/windfall-nextcloud-flow-windmill-rce/)

## License

[WTFPL](http://www.wtfpl.net/) - Do What The Fuck You Want To Public License
