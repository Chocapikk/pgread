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
pgread -d /path/to/data/              # Dump all
pgread -d /path/to/data/ -db mydb     # Specific database
pgread -d /path/to/data/ -t password  # Filter tables
pgread -d /path/to/data/ -list        # Schema only
pgread -f /path/to/1262               # Parse single file
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

MIT
