package pkg

// System catalog OIDs (fixed in all PostgreSQL versions)
const (
	PGType        = 1247 // pg_type - data types
	PGAttribute   = 1249 // pg_attribute - table columns
	PGClass       = 1259 // pg_class - tables/indexes
	PGAuthID      = 1260 // pg_authid - users/roles (global)
	PGAuthMembers = 1261 // pg_auth_members - role membership (global)
	PGDatabase    = 1262 // pg_database - databases (global)
)

// Column represents a table column definition
type Column struct {
	Name  string
	TypID int
	Len   int
	Num   int
}

// pg_database schema (partial - what we need)
var PGDatabaseSchema = []Column{
	{Name: "oid", TypID: OidOid, Len: 4},
	{Name: "datname", TypID: OidName, Len: 64},
}

// pg_class schema (PostgreSQL 12+)
var PGClassSchema = []Column{
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

// pg_attribute schema varies between versions
var PGAttributeSchemaV15 = []Column{
	{Name: "attrelid", TypID: OidOid, Len: 4},
	{Name: "attname", TypID: OidName, Len: 64},
	{Name: "atttypid", TypID: OidOid, Len: 4},
	{Name: "attstattarget", TypID: OidInt4, Len: 4},
	{Name: "attlen", TypID: OidInt2, Len: 2},
	{Name: "attnum", TypID: OidInt2, Len: 2},
}

var PGAttributeSchemaV16 = []Column{
	{Name: "attrelid", TypID: OidOid, Len: 4},
	{Name: "attname", TypID: OidName, Len: 64},
	{Name: "atttypid", TypID: OidOid, Len: 4},
	{Name: "attlen", TypID: OidInt2, Len: 2},
	{Name: "attnum", TypID: OidInt2, Len: 2},
}

// TableInfo represents parsed pg_class entry
type TableInfo struct {
	OID      uint32
	Name     string
	Filenode uint32
	Kind     string
}

// AttrInfo represents parsed pg_attribute entry
type AttrInfo struct {
	Name  string
	TypID int
	Num   int
	Len   int
}

// DatabaseInfo represents parsed pg_database entry
type DatabaseInfo struct {
	OID  uint32
	Name string
}

// ParsePGDatabase parses pg_database tuples
func ParsePGDatabase(data []byte) []DatabaseInfo {
	tuples := ReadTuples(data, true)
	var databases []DatabaseInfo

	for _, t := range tuples {
		row := DecodeTuple(t.Tuple, PGDatabaseSchema)
		if row == nil {
			continue
		}

		oid, _ := row["oid"].(uint32)
		name, _ := row["datname"].(string)

		if oid > 0 && name != "" {
			databases = append(databases, DatabaseInfo{OID: oid, Name: name})
		}
	}

	return databases
}

// ParsePGClass parses pg_class tuples
func ParsePGClass(data []byte) map[uint32]TableInfo {
	tuples := ReadTuples(data, true)
	tables := make(map[uint32]TableInfo)

	for _, t := range tuples {
		row := DecodeTuple(t.Tuple, PGClassSchema)
		if row == nil {
			continue
		}

		filenode, _ := row["relfilenode"].(uint32)
		if filenode == 0 {
			continue
		}

		oid, _ := row["oid"].(uint32)
		name, _ := row["relname"].(string)
		kind, _ := row["relkind"].(string)

		tables[filenode] = TableInfo{
			OID:      oid,
			Name:     name,
			Filenode: filenode,
			Kind:     kind,
		}
	}

	return tables
}

// ParsePGAttribute parses pg_attribute tuples
func ParsePGAttribute(data []byte, pgVersion int) map[uint32][]AttrInfo {
	schema := selectAttributeSchema(data, pgVersion)
	tuples := ReadTuples(data, true)

	result := make(map[uint32][]AttrInfo)

	for _, t := range tuples {
		row := DecodeTuple(t.Tuple, schema)
		if row == nil {
			continue
		}

		relid, _ := row["attrelid"].(uint32)
		attnum := getInt(row["attnum"])

		if relid == 0 || attnum <= 0 {
			continue
		}

		name, _ := row["attname"].(string)
		typid, _ := row["atttypid"].(uint32)
		attlen := getInt(row["attlen"])

		result[relid] = append(result[relid], AttrInfo{
			Name:  name,
			TypID: int(typid),
			Num:   attnum,
			Len:   attlen,
		})
	}

	// Sort columns by attnum
	for relid := range result {
		attrs := result[relid]
		// Simple insertion sort
		for i := 1; i < len(attrs); i++ {
			for j := i; j > 0 && attrs[j].Num < attrs[j-1].Num; j-- {
				attrs[j], attrs[j-1] = attrs[j-1], attrs[j]
			}
		}
		result[relid] = attrs
	}

	return result
}

func selectAttributeSchema(data []byte, pgVersion int) []Column {
	if pgVersion >= 16 {
		return PGAttributeSchemaV16
	}
	if pgVersion >= 12 && pgVersion <= 15 {
		return PGAttributeSchemaV15
	}

	// Auto-detect: try V16 schema first
	tuples := ReadTuples(data, true)
	if len(tuples) < 5 {
		return PGAttributeSchemaV15 // Default
	}

	v16Nums := make([]int, 0, 5)
	for i := 0; i < 5 && i < len(tuples); i++ {
		row := DecodeTuple(tuples[i].Tuple, PGAttributeSchemaV16)
		if row != nil {
			v16Nums = append(v16Nums, getInt(row["attnum"]))
		}
	}

	// If V16 schema gives sequential 1,2,3,4,5 it's correct
	if len(v16Nums) == 5 &&
		v16Nums[0] == 1 && v16Nums[1] == 2 && v16Nums[2] == 3 &&
		v16Nums[3] == 4 && v16Nums[4] == 5 {
		return PGAttributeSchemaV16
	}

	return PGAttributeSchemaV15
}

func getInt(v interface{}) int {
	switch val := v.(type) {
	case int:
		return val
	case int16:
		return int(val)
	case int32:
		return int(val)
	case int64:
		return int(val)
	case uint32:
		return int(val)
	default:
		return 0
	}
}

// BuildSchema builds complete schema from pg_class and pg_attribute
func BuildSchema(classData, attrData []byte) map[uint32]struct {
	Name    string
	Kind    string
	Columns []AttrInfo
} {
	tables := ParsePGClass(classData)
	columns := ParsePGAttribute(attrData, 0)

	result := make(map[uint32]struct {
		Name    string
		Kind    string
		Columns []AttrInfo
	})

	for filenode, info := range tables {
		result[filenode] = struct {
			Name    string
			Kind    string
			Columns []AttrInfo
		}{
			Name:    info.Name,
			Kind:    info.Kind,
			Columns: columns[info.OID],
		}
	}

	return result
}
