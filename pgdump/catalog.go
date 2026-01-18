package pgdump

import "sort"

// System catalog OIDs (fixed in all PostgreSQL versions)
const (
	PGDatabase  = 1262 // pg_database - databases (global)
	PGClass     = 1259 // pg_class - tables/indexes
	PGAttribute = 1249 // pg_attribute - table columns
)

// Column defines a table column for decoding
type Column struct {
	Name  string
	TypID int
	Len   int
	Num   int
}

// DatabaseInfo represents a database entry
type DatabaseInfo struct {
	OID  uint32
	Name string
}

// TableInfo represents a table entry
type TableInfo struct {
	OID, Filenode uint32
	Name, Kind    string
}

// AttrInfo represents a column attribute
type AttrInfo struct {
	Name  string
	TypID int
	Num   int
	Len   int
}

// Predefined schemas for system catalogs
var (
	schemaPGDatabase = []Column{
		{Name: "oid", TypID: OidOid, Len: 4},
		{Name: "datname", TypID: OidName, Len: 64},
	}

	schemaPGClass = []Column{
		{Name: "oid", TypID: OidOid, Len: 4},
		{Name: "relname", TypID: OidName, Len: 64},
		{Name: "relnamespace", TypID: OidOid, Len: 4},
		{Name: "reltype", TypID: OidOid, Len: 4},
		{Name: "reloftype", TypID: OidOid, Len: 4},
		{Name: "relowner", TypID: OidOid, Len: 4},
		{Name: "relam", TypID: OidOid, Len: 4},
		{Name: "relfilenode", TypID: OidOid, Len: 4},
		{Name: "reltablespace", TypID: OidOid, Len: 4},
		{Name: "relpages", TypID: OidInt4, Len: 4},
		{Name: "reltuples", TypID: OidFloat4, Len: 4},
		{Name: "relallvisible", TypID: OidInt4, Len: 4},
		{Name: "reltoastrelid", TypID: OidOid, Len: 4},
		{Name: "relhasindex", TypID: OidBool, Len: 1},
		{Name: "relisshared", TypID: OidBool, Len: 1},
		{Name: "relpersistence", TypID: OidChar, Len: 1},
		{Name: "relkind", TypID: OidChar, Len: 1},
	}

	schemaPGAttrV15 = []Column{
		{Name: "attrelid", TypID: OidOid, Len: 4},
		{Name: "attname", TypID: OidName, Len: 64},
		{Name: "atttypid", TypID: OidOid, Len: 4},
		{Name: "attstattarget", TypID: OidInt4, Len: 4},
		{Name: "attlen", TypID: OidInt2, Len: 2},
		{Name: "attnum", TypID: OidInt2, Len: 2},
	}

	schemaPGAttrV16 = []Column{
		{Name: "attrelid", TypID: OidOid, Len: 4},
		{Name: "attname", TypID: OidName, Len: 64},
		{Name: "atttypid", TypID: OidOid, Len: 4},
		{Name: "attlen", TypID: OidInt2, Len: 2},
		{Name: "attnum", TypID: OidInt2, Len: 2},
	}
)

// ParsePGDatabase extracts database list from pg_database heap file
func ParsePGDatabase(data []byte) []DatabaseInfo {
	var result []DatabaseInfo
	for _, row := range ReadRows(data, schemaPGDatabase, true) {
		if oid, name := getOID(row, "oid"), getString(row, "datname"); oid > 0 && name != "" {
			result = append(result, DatabaseInfo{OID: oid, Name: name})
		}
	}
	return result
}

// ParsePGClass extracts table info from pg_class heap file
func ParsePGClass(data []byte) map[uint32]TableInfo {
	tables := make(map[uint32]TableInfo)
	for _, row := range ReadRows(data, schemaPGClass, true) {
		if fn := getOID(row, "relfilenode"); fn > 0 {
			tables[fn] = TableInfo{
				OID:      getOID(row, "oid"),
				Name:     getString(row, "relname"),
				Filenode: fn,
				Kind:     getString(row, "relkind"),
			}
		}
	}
	return tables
}

// ParsePGAttribute extracts column info from pg_attribute heap file
func ParsePGAttribute(data []byte, pgVersion int) map[uint32][]AttrInfo {
	schema := detectAttrSchema(data, pgVersion)
	result := make(map[uint32][]AttrInfo)

	for _, row := range ReadRows(data, schema, true) {
		relid, num := getOID(row, "attrelid"), toInt(row["attnum"])
		if relid == 0 || num <= 0 {
			continue
		}
		result[relid] = append(result[relid], AttrInfo{
			Name:  getString(row, "attname"),
			TypID: int(getOID(row, "atttypid")),
			Num:   num,
			Len:   toInt(row["attlen"]),
		})
	}

	// Sort by attnum
	for relid := range result {
		sort.Slice(result[relid], func(i, j int) bool {
			return result[relid][i].Num < result[relid][j].Num
		})
	}
	return result
}

func detectAttrSchema(data []byte, version int) []Column {
	if version >= 16 {
		return schemaPGAttrV16
	}
	if version >= 12 {
		return schemaPGAttrV15
	}

	// Auto-detect by trying V16 schema
	rows := ReadRows(data, schemaPGAttrV16, true)
	if len(rows) >= 5 {
		match := true
		for i := 0; i < 5; i++ {
			if toInt(rows[i]["attnum"]) != i+1 {
				match = false
				break
			}
		}
		if match {
			return schemaPGAttrV16
		}
	}
	return schemaPGAttrV15
}

func getOID(row map[string]interface{}, key string) uint32 {
	if v, ok := row[key].(uint32); ok {
		return v
	}
	return 0
}

func getString(row map[string]interface{}, key string) string {
	if v, ok := row[key].(string); ok {
		return v
	}
	return ""
}
