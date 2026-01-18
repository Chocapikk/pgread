# pgdump-offline

Dump PostgreSQL data from leaked heap files - no credentials needed.

## The Technique

PostgreSQL uses **fixed OIDs** for system catalogs:
- `1262` - pg_database (list all databases)
- `1259` - pg_class (list all tables)  
- `1249` - pg_attribute (list all columns)

With a file read vulnerability, leak these 3 files to discover the entire schema, then dump any table.

## Installation

```bash
go install github.com/chocapikk/pgdump-offline@latest
```

Or download pre-built binaries from releases.

## Usage

```bash
# Auto-discover and dump everything
pgdump-offline -d /path/to/pg_data/

# List databases and tables only
pgdump-offline -d /path/to/pg_data/ -list

# Dump specific database
pgdump-offline -d /path/to/pg_data/ -db windmill

# Filter tables by name
pgdump-offline -d /path/to/pg_data/ -t password
pgdump-offline -d /path/to/pg_data/ -t token
pgdump-offline -d /path/to/pg_data/ -t secret

# Parse single file
pgdump-offline -f /path/to/1262  # pg_database
pgdump-offline -f /path/to/1259  # pg_class
pgdump-offline -f /path/to/1249  # pg_attribute
```

## Example Output

```json
{
  "databases": [
    {
      "oid": 16384,
      "name": "windmill",
      "tables": [
        {
          "name": "password",
          "columns": [
            {"name": "email", "type": "varchar"},
            {"name": "password_hash", "type": "varchar"}
          ],
          "rows": [
            {
              "email": "admin@windmill.dev",
              "password_hash": "$argon2id$v=19$m=4096,t=3,p=1$..."
            }
          ]
        }
      ]
    }
  ]
}
```

## Supported Types

- **Scalars**: bool, int2, int4, int8, float4, float8, text, varchar, name, char, oid
- **Date/Time**: date, time, timestamp, timestamptz, interval
- **Binary**: bytea, uuid
- **Network**: inet, macaddr
- **Complex**: jsonb (fully parsed), numeric, arrays

## File Locations

```
/var/lib/postgresql/data/           # Default Linux
/var/lib/postgresql/15/main/        # Debian/Ubuntu
C:\Program Files\PostgreSQL\data\   # Windows

global/1262                         # pg_database
base/<db_oid>/1259                  # pg_class  
base/<db_oid>/1249                  # pg_attribute
base/<db_oid>/<filenode>            # Table data
```

## How It Works

1. Parse `global/1262` to find databases and their OIDs
2. For each database, parse `base/<oid>/1259` to find tables
3. Parse `base/<oid>/1249` to get column definitions
4. Use column schema to decode table data files

No SQL queries, no authentication - pure binary parsing.

## Building

```bash
git clone https://github.com/chocapikk/pgdump-offline
cd pgdump-offline
go build

# Cross-compile
GOOS=windows GOARCH=amd64 go build -o pgdump-offline.exe
GOOS=darwin GOARCH=arm64 go build -o pgdump-offline-macos
GOOS=linux GOARCH=amd64 go build -o pgdump-offline-linux
```

## Credits

Based on research into PostgreSQL internals for the Windfall vulnerability chain.
Ported from Ruby (Metasploit Rex::Proto::PostgreSQL) to Go.

## License

MIT
