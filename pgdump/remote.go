package pgdump

import (
	"fmt"
	"strings"
)

// RemoteReader reads files from a PostgreSQL data directory given relative paths
// Example paths: "global/1260", "base/16384/1259", "PG_VERSION"
type RemoteReader func(path string) ([]byte, error)

// RemoteClient provides a high-level interface to explore PostgreSQL data remotely
type RemoteClient struct {
	reader  RemoteReader
	version int
	cache   struct {
		databases []DatabaseInfo
		tables    map[uint32]map[uint32]TableInfo // db OID -> filenode -> table
		columns   map[uint32]map[uint32][]AttrInfo // db OID -> relid -> columns
	}
}

// NewRemoteClient creates a new remote client with the given reader
func NewRemoteClient(reader RemoteReader) *RemoteClient {
	c := &RemoteClient{reader: reader}
	c.cache.tables = make(map[uint32]map[uint32]TableInfo)
	c.cache.columns = make(map[uint32]map[uint32][]AttrInfo)

	// Detect version
	if data, err := reader("PG_VERSION"); err == nil {
		v := strings.TrimSpace(string(data))
		fmt.Sscanf(v, "%d", &c.version)
	}

	return c
}

// Version returns the PostgreSQL version string
func (c *RemoteClient) Version() string {
	if data, err := c.reader("PG_VERSION"); err == nil {
		return strings.TrimSpace(string(data))
	}
	return ""
}

// Control returns the pg_control file info
func (c *RemoteClient) Control() *ControlFile {
	if data, err := c.reader("global/pg_control"); err == nil {
		if ctrl, err := ParseControlFile(data); err == nil {
			return ctrl
		}
	}
	return nil
}

// Credentials returns all user credentials (pg_authid)
func (c *RemoteClient) Credentials() []AuthInfo {
	if data, err := c.reader(fmt.Sprintf("global/%d", PGAuthID)); err == nil {
		return ParsePGAuthID(data)
	}
	return nil
}

// Databases returns all databases
func (c *RemoteClient) Databases() []DatabaseInfo {
	if c.cache.databases != nil {
		return c.cache.databases
	}
	if data, err := c.reader(fmt.Sprintf("global/%d", PGDatabase)); err == nil {
		c.cache.databases = ParsePGDatabase(data)
	}
	return c.cache.databases
}

// Database returns a specific database by name
func (c *RemoteClient) Database(name string) *DatabaseInfo {
	for _, db := range c.Databases() {
		if strings.EqualFold(db.Name, name) {
			return &db
		}
	}
	return nil
}

// loadCatalog loads pg_class and pg_attribute for a database
func (c *RemoteClient) loadCatalog(dbOID uint32) {
	if _, ok := c.cache.tables[dbOID]; ok {
		return
	}

	base := fmt.Sprintf("base/%d", dbOID)

	// Load tables
	classData, err := c.reader(fmt.Sprintf("%s/%d", base, PGClass))
	if err != nil {
		c.cache.tables[dbOID] = make(map[uint32]TableInfo)
		c.cache.columns[dbOID] = make(map[uint32][]AttrInfo)
		return
	}
	c.cache.tables[dbOID] = ParsePGClass(classData)

	// Load columns
	attrData, err := c.reader(fmt.Sprintf("%s/%d", base, PGAttribute))
	if err != nil {
		c.cache.columns[dbOID] = make(map[uint32][]AttrInfo)
		return
	}
	c.cache.columns[dbOID] = ParsePGAttribute(attrData, c.version)
}

// Tables returns all tables in a database
func (c *RemoteClient) Tables(dbOID uint32) []TableInfo {
	c.loadCatalog(dbOID)
	var tables []TableInfo
	for _, t := range c.cache.tables[dbOID] {
		tables = append(tables, t)
	}
	return tables
}

// TablesByName returns all tables in a database by name
func (c *RemoteClient) TablesByName(dbName string) []TableInfo {
	db := c.Database(dbName)
	if db == nil {
		return nil
	}
	return c.Tables(db.OID)
}

// Table returns a specific table by name
func (c *RemoteClient) Table(dbOID uint32, tableName string) *TableInfo {
	c.loadCatalog(dbOID)
	for _, t := range c.cache.tables[dbOID] {
		if strings.EqualFold(t.Name, tableName) {
			return &t
		}
	}
	return nil
}

// Columns returns all columns for a table
func (c *RemoteClient) Columns(dbOID, tableOID uint32) []AttrInfo {
	c.loadCatalog(dbOID)
	return c.cache.columns[dbOID][tableOID]
}

// ColumnNames returns just column names for a table
func (c *RemoteClient) ColumnNames(dbOID, tableOID uint32) []string {
	var names []string
	for _, col := range c.Columns(dbOID, tableOID) {
		if col.Num > 0 { // Skip system columns
			names = append(names, col.Name)
		}
	}
	return names
}

// QueryOptions configures data extraction
type QueryOptions struct {
	Columns []string // Specific columns to extract (empty = all)
	Limit   int      // Max rows (0 = unlimited)
}

// Query extracts data from a table
func (c *RemoteClient) Query(dbOID uint32, table *TableInfo, opts *QueryOptions) []map[string]any {
	if table == nil || table.Filenode == 0 {
		return nil
	}

	base := fmt.Sprintf("base/%d", dbOID)
	data, err := c.reader(fmt.Sprintf("%s/%d", base, table.Filenode))
	if err != nil {
		return nil
	}

	attrs := c.Columns(dbOID, table.OID)
	cols := make([]Column, len(attrs))
	for i, a := range attrs {
		cols[i] = Column{Name: a.Name, TypID: a.TypID, Len: a.Len, Num: a.Num, Align: a.Align}
	}
	rows := ReadRows(data, cols, true)

	// Filter columns if specified
	if opts != nil && len(opts.Columns) > 0 {
		filtered := make([]map[string]any, 0, len(rows))
		for _, row := range rows {
			newRow := make(map[string]any)
			for _, col := range opts.Columns {
				if val, ok := row[col]; ok {
					newRow[col] = val
				}
			}
			filtered = append(filtered, newRow)
		}
		rows = filtered
	}

	// Apply limit
	if opts != nil && opts.Limit > 0 && len(rows) > opts.Limit {
		rows = rows[:opts.Limit]
	}

	return rows
}

// QueryByName extracts data from a table by database and table name
func (c *RemoteClient) QueryByName(dbName, tableName string, opts *QueryOptions) []map[string]any {
	db := c.Database(dbName)
	if db == nil {
		return nil
	}
	table := c.Table(db.OID, tableName)
	return c.Query(db.OID, table, opts)
}

// DumpTable dumps a complete table with metadata
func (c *RemoteClient) DumpTable(dbOID uint32, table *TableInfo) *TableDump {
	if table == nil {
		return nil
	}
	rows := c.Query(dbOID, table, nil)

	// Convert column names to ColumnInfo
	var cols []ColumnInfo
	for _, a := range c.Columns(dbOID, table.OID) {
		if a.Num > 0 {
			cols = append(cols, ColumnInfo{Name: a.Name, TypID: a.TypID, Type: TypeName(a.TypID)})
		}
	}

	return &TableDump{
		OID:      table.OID,
		Name:     table.Name,
		Filenode: table.Filenode,
		Kind:     table.Kind,
		Columns:  cols,
		Rows:     rows,
		RowCount: len(rows),
	}
}

// DumpDatabase dumps all user tables in a database
func (c *RemoteClient) DumpDatabase(dbOID uint32) *DatabaseDump {
	db := c.findDB(dbOID)
	if db == nil {
		return nil
	}

	dump := &DatabaseDump{OID: dbOID, Name: db.Name}
	for _, t := range c.Tables(dbOID) {
		if strings.HasPrefix(t.Name, "pg_") || strings.HasPrefix(t.Name, "sql_") {
			continue
		}
		if td := c.DumpTable(dbOID, &t); td != nil && len(td.Rows) > 0 {
			dump.Tables = append(dump.Tables, *td)
		}
	}
	return dump
}

// DumpDatabaseByName dumps a database by name
func (c *RemoteClient) DumpDatabaseByName(name string) *DatabaseDump {
	db := c.Database(name)
	if db == nil {
		return nil
	}
	return c.DumpDatabase(db.OID)
}

// DumpAll dumps everything
func (c *RemoteClient) DumpAll() *DumpResult {
	result := &DumpResult{}
	for _, db := range c.Databases() {
		if strings.HasPrefix(db.Name, "template") {
			continue
		}
		if dump := c.DumpDatabase(db.OID); dump != nil {
			result.Databases = append(result.Databases, *dump)
		}
	}
	return result
}

func (c *RemoteClient) findDB(oid uint32) *DatabaseInfo {
	for _, db := range c.Databases() {
		if db.OID == oid {
			return &db
		}
	}
	return nil
}

// Summary is a lightweight overview
type Summary struct {
	Version     string              `json:"version,omitempty"`
	Credentials []string            `json:"credentials,omitempty"`
	Databases   map[string][]string `json:"databases,omitempty"`
}

// Summary returns a lightweight overview of the PostgreSQL instance
func (c *RemoteClient) Summary() *Summary {
	s := &Summary{
		Version:   c.Version(),
		Databases: make(map[string][]string),
	}

	for _, cred := range c.Credentials() {
		if cred.Password != "" {
			s.Credentials = append(s.Credentials, cred.RoleName+":"+cred.Password)
		}
	}

	for _, db := range c.Databases() {
		if strings.HasPrefix(db.Name, "template") {
			continue
		}
		for _, t := range c.Tables(db.OID) {
			if !strings.HasPrefix(t.Name, "pg_") && !strings.HasPrefix(t.Name, "sql_") {
				s.Databases[db.Name] = append(s.Databases[db.Name], t.Name)
			}
		}
	}

	return s
}

// Exec executes a command and returns the result as any (for JSON output)
func (c *RemoteClient) Exec(args []string) any {
	if len(args) == 0 {
		return c.Summary()
	}

	cmd := args[0]
	switch cmd {
	case "summary":
		return c.Summary()
	case "creds", "credentials":
		return c.Credentials()
	case "dbs", "databases":
		return c.Databases()
	case "control":
		return c.Control()
	case "version":
		return c.Version()
	case "tables":
		if len(args) < 2 {
			return nil
		}
		return c.TablesByName(args[1])
	case "columns":
		if len(args) < 3 {
			return nil
		}
		db := c.Database(args[1])
		if db == nil {
			return nil
		}
		table := c.Table(db.OID, args[2])
		if table == nil {
			return nil
		}
		return c.Columns(db.OID, table.OID)
	case "query":
		if len(args) < 3 {
			return nil
		}
		return c.QueryByName(args[1], args[2], nil)
	case "dump":
		if len(args) >= 2 {
			return c.DumpDatabaseByName(args[1])
		}
		return c.DumpAll()
	default:
		return nil
	}
}

// ExecPretty executes a command and returns a human-readable string
func (c *RemoteClient) ExecPretty(args []string) string {
	var b strings.Builder
	cmd := ""
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "", "summary":
		b.WriteString(fmt.Sprintf("PostgreSQL %s\n\n", c.Version()))
		if creds := c.Credentials(); len(creds) > 0 {
			b.WriteString("CREDENTIALS\n")
			for _, cr := range creds {
				if cr.Password != "" {
					role := ""
					if cr.RolSuper {
						role = " [superuser]"
					}
					b.WriteString(fmt.Sprintf("  %s%s\n    %s\n", cr.RoleName, role, cr.Password))
				}
			}
			b.WriteString("\n")
		}
		b.WriteString("DATABASES\n")
		for _, db := range c.Databases() {
			if strings.HasPrefix(db.Name, "template") {
				continue
			}
			tables := c.Tables(db.OID)
			userTables := 0
			var tableNames []string
			for _, t := range tables {
				if !strings.HasPrefix(t.Name, "pg_") && t.Kind == "r" {
					userTables++
					if !strings.HasPrefix(t.Name, "sql_") {
						tableNames = append(tableNames, t.Name)
					}
				}
			}
			if len(tableNames) > 0 {
				b.WriteString(fmt.Sprintf("  %s: %s\n", db.Name, strings.Join(tableNames, ", ")))
			} else {
				b.WriteString(fmt.Sprintf("  %s (%d tables)\n", db.Name, userTables))
			}
		}

	case "creds", "credentials":
		for _, cr := range c.Credentials() {
			if cr.Password != "" {
				b.WriteString(fmt.Sprintf("%s:%s\n", cr.RoleName, cr.Password))
			}
		}

	case "dbs", "databases":
		b.WriteString("NAME                 OID\n")
		for _, db := range c.Databases() {
			b.WriteString(fmt.Sprintf("%-20s %d\n", db.Name, db.OID))
		}

	case "tables":
		if len(args) < 2 {
			return "usage: tables <database>"
		}
		tables := c.TablesByName(args[1])
		b.WriteString("NAME                           KIND   OID\n")
		for _, t := range tables {
			if t.Kind == "r" {
				b.WriteString(fmt.Sprintf("%-30s table  %d\n", t.Name, t.OID))
			}
		}

	case "columns":
		if len(args) < 3 {
			return "usage: columns <database> <table>"
		}
		db := c.Database(args[1])
		if db == nil {
			return "database not found"
		}
		table := c.Table(db.OID, args[2])
		if table == nil {
			return "table not found"
		}
		b.WriteString("NAME                 TYPE\n")
		for _, col := range c.Columns(db.OID, table.OID) {
			if col.Num > 0 {
				b.WriteString(fmt.Sprintf("%-20s %s\n", col.Name, TypeName(col.TypID)))
			}
		}

	case "query":
		if len(args) < 3 {
			return "usage: query <database> <table>"
		}
		rows := c.QueryByName(args[1], args[2], &QueryOptions{Limit: 20})
		if len(rows) == 0 {
			return "no data"
		}
		// Get columns from first row
		var cols []string
		for k := range rows[0] {
			cols = append(cols, k)
		}
		// Header
		for _, col := range cols {
			b.WriteString(fmt.Sprintf("%-20s", truncate(col, 20)))
		}
		b.WriteString("\n")
		// Rows
		for _, row := range rows {
			for _, col := range cols {
				val := fmt.Sprintf("%v", row[col])
				b.WriteString(fmt.Sprintf("%-20s", truncate(val, 20)))
			}
			b.WriteString("\n")
		}

	case "dump":
		if len(args) >= 2 {
			dump := c.DumpDatabaseByName(args[1])
			if dump != nil {
				b.WriteString(formatDump(dump))
			}
		} else {
			for _, db := range c.Databases() {
				if strings.HasPrefix(db.Name, "template") {
					continue
				}
				if dump := c.DumpDatabase(db.OID); dump != nil {
					b.WriteString(formatDump(dump))
					b.WriteString("\n")
				}
			}
		}

	default:
		return "unknown command"
	}

	return b.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-2] + ".."
}

func formatDump(dump *DatabaseDump) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("=== %s ===\n", dump.Name))
	for _, t := range dump.Tables {
		b.WriteString(fmt.Sprintf("\n[%s] %d rows\n", t.Name, len(t.Rows)))
		if len(t.Rows) == 0 {
			continue
		}
		// Get columns
		var cols []string
		for k := range t.Rows[0] {
			cols = append(cols, k)
		}
		for _, col := range cols {
			b.WriteString(fmt.Sprintf("%-20s", truncate(col, 20)))
		}
		b.WriteString("\n")
		for _, row := range t.Rows {
			for _, col := range cols {
				val := fmt.Sprintf("%v", row[col])
				b.WriteString(fmt.Sprintf("%-20s", truncate(val, 20)))
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}
