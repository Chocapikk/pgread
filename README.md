# pgdump-offline

[![Test](https://github.com/Chocapikk/pgdump-offline/actions/workflows/test.yml/badge.svg)](https://github.com/Chocapikk/pgdump-offline/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/Chocapikk/pgdump-offline/branch/main/graph/badge.svg)](https://codecov.io/gh/Chocapikk/pgdump-offline)
[![Go Report Card](https://goreportcard.com/badge/github.com/Chocapikk/pgdump-offline)](https://goreportcard.com/report/github.com/Chocapikk/pgdump-offline)

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
go install github.com/Chocapikk/pgdump-offline@latest
```

## CLI

```bash
pgdump-offline -d /path/to/data/              # Dump all
pgdump-offline -d /path/to/data/ -db mydb     # Specific database
pgdump-offline -d /path/to/data/ -t password  # Filter tables
pgdump-offline -d /path/to/data/ -list        # Schema only
pgdump-offline -f /path/to/1262               # Parse single file
```

## Library

```go
import "github.com/Chocapikk/pgdump-offline/pgdump"

// Simple
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

## Low-Level API

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
GOOS=windows go build -o pgdump-offline.exe
GOOS=darwin GOARCH=arm64 go build -o pgdump-offline-macos
```

## License

MIT
