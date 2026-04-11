package pgdump

import (
	"encoding/json"
	"fmt"
	"strings"
)

// RemoteReader reads files from a PostgreSQL data directory given relative paths
type RemoteReader func(path string) ([]byte, error)

// RemoteClient provides a high-level interface to explore PostgreSQL data remotely
type RemoteClient struct {
	reader  RemoteReader
	version int
	cache   struct {
		databases []DatabaseInfo
		tables    map[uint32]map[uint32]TableInfo
		columns   map[uint32]map[uint32][]AttrInfo
	}
}

// NewRemoteClient creates a new remote client with the given reader
func NewRemoteClient(reader RemoteReader) *RemoteClient {
	c := &RemoteClient{reader: reader}
	c.cache.tables = make(map[uint32]map[uint32]TableInfo)
	c.cache.columns = make(map[uint32]map[uint32][]AttrInfo)
	if data, err := reader("PG_VERSION"); err == nil {
		fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &c.version)
	}
	return c
}

// Result is the interface for all command results
type Result interface {
	String() string
}

// --- Result Types ---

type VersionResult string

func (v VersionResult) String() string { return string(v) }

type ControlResult struct{ *ControlFile }

func (c ControlResult) String() string {
	if c.ControlFile == nil {
		return "could not read pg_control"
	}
	return fmt.Sprintf("PostgreSQL %d\nState: %s\nCheckpoint: %s\nTimeline: %d\nSystem ID: %d",
		c.PGVersionMajor, c.StateString, c.CheckpointLSN, c.TimeLineID, c.SystemIdentifier)
}

type CredsResult []AuthInfo

func (c CredsResult) String() string {
	var b strings.Builder
	for _, cr := range c {
		if cr.Password != "" {
			b.WriteString(fmt.Sprintf("%s:%s\n", cr.RoleName, cr.Password))
		}
	}
	return b.String()
}

type DatabasesResult []DatabaseInfo

func (d DatabasesResult) String() string {
	var b strings.Builder
	b.WriteString("NAME                 OID\n")
	for _, db := range d {
		b.WriteString(fmt.Sprintf("%-20s %d\n", db.Name, db.OID))
	}
	return b.String()
}

type TablesResult []TableInfo

func (t TablesResult) String() string {
	var b strings.Builder
	b.WriteString("NAME                           KIND   OID\n")
	for _, tbl := range t {
		if tbl.Kind == "r" {
			b.WriteString(fmt.Sprintf("%-30s table  %d\n", tbl.Name, tbl.OID))
		}
	}
	return b.String()
}

type ColumnsResult []AttrInfo

func (c ColumnsResult) String() string {
	var b strings.Builder
	b.WriteString("NAME                 TYPE\n")
	for _, col := range c {
		if col.Num > 0 {
			b.WriteString(fmt.Sprintf("%-20s %s\n", col.Name, TypeName(col.TypID)))
		}
	}
	return b.String()
}

type QueryResult []map[string]any

func (q QueryResult) String() string {
	if len(q) == 0 {
		return "no data"
	}
	var b strings.Builder
	var cols []string
	for k := range q[0] {
		cols = append(cols, k)
	}
	for _, col := range cols {
		b.WriteString(fmt.Sprintf("%-20s", truncate(col, 20)))
	}
	b.WriteString("\n")
	for _, row := range q {
		for _, col := range cols {
			b.WriteString(fmt.Sprintf("%-20s", truncate(fmt.Sprintf("%v", row[col]), 20)))
		}
		b.WriteString("\n")
	}
	return b.String()
}

type DumpDatabaseResult struct{ *DatabaseDump }

func (d DumpDatabaseResult) String() string {
	if d.DatabaseDump == nil {
		return ""
	}
	return formatDump(d.DatabaseDump)
}

type DumpAllResult struct{ *DumpResult }

func (d DumpAllResult) String() string {
	if d.DumpResult == nil {
		return ""
	}
	var b strings.Builder
	for _, db := range d.Databases {
		b.WriteString(formatDump(&db))
		b.WriteString("\n")
	}
	return b.String()
}

type SummaryResult struct {
	version string
	creds   []AuthInfo
	dbs     []DatabaseInfo
	tables  map[uint32][]TableInfo
}

func (s SummaryResult) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("PostgreSQL %s\n\n", s.version))

	hasCreds := false
	for _, cr := range s.creds {
		if cr.Password != "" {
			if !hasCreds {
				b.WriteString("CREDENTIALS\n")
				hasCreds = true
			}
			role := ""
			if cr.RolSuper {
				role = " [superuser]"
			}
			b.WriteString(fmt.Sprintf("  %s%s\n    %s\n", cr.RoleName, role, cr.Password))
		}
	}
	if hasCreds {
		b.WriteString("\n")
	}

	b.WriteString("DATABASES\n")
	for _, db := range s.dbs {
		if isTemplateDB(db.Name) {
			continue
		}
		tables := s.tables[db.OID]
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
	return b.String()
}

func (s SummaryResult) MarshalJSON() ([]byte, error) {
	summary := &Summary{Version: s.version, Databases: make(map[string][]string)}
	for _, cr := range s.creds {
		if cr.Password != "" {
			summary.Credentials = append(summary.Credentials, cr.RoleName+":"+cr.Password)
		}
	}
	for _, db := range s.dbs {
		if isTemplateDB(db.Name) {
			continue
		}
		for _, t := range s.tables[db.OID] {
			if !strings.HasPrefix(t.Name, "pg_") && !strings.HasPrefix(t.Name, "sql_") && t.Kind == "r" {
				summary.Databases[db.Name] = append(summary.Databases[db.Name], t.Name)
			}
		}
	}
	return json.Marshal(summary)
}

type ErrorResult string

func (e ErrorResult) String() string { return string(e) }

// --- Client Methods ---

func (c *RemoteClient) Version() string {
	if data, err := c.reader("PG_VERSION"); err == nil {
		return strings.TrimSpace(string(data))
	}
	return ""
}

func (c *RemoteClient) Control() *ControlFile {
	if data, err := c.reader("global/pg_control"); err == nil {
		if ctrl, err := ParseControlFile(data); err == nil {
			return ctrl
		}
	}
	return nil
}

func (c *RemoteClient) Credentials() []AuthInfo {
	if data, err := c.reader(fmt.Sprintf("global/%d", PGAuthID)); err == nil {
		return ParsePGAuthID(data)
	}
	return nil
}

func (c *RemoteClient) Databases() []DatabaseInfo {
	if c.cache.databases != nil {
		return c.cache.databases
	}
	if data, err := c.reader(fmt.Sprintf("global/%d", PGDatabase)); err == nil {
		c.cache.databases = ParsePGDatabase(data)
	}
	return c.cache.databases
}

func (c *RemoteClient) Database(name string) *DatabaseInfo {
	for _, db := range c.Databases() {
		if strings.EqualFold(db.Name, name) {
			return &db
		}
	}
	return nil
}

func (c *RemoteClient) loadCatalog(dbOID uint32) {
	if _, ok := c.cache.tables[dbOID]; ok {
		return
	}
	base := fmt.Sprintf("base/%d", dbOID)
	classData, err := c.reader(fmt.Sprintf("%s/%d", base, PGClass))
	if err != nil {
		c.cache.tables[dbOID] = make(map[uint32]TableInfo)
		c.cache.columns[dbOID] = make(map[uint32][]AttrInfo)
		return
	}
	c.cache.tables[dbOID] = ParsePGClass(classData)
	attrData, err := c.reader(fmt.Sprintf("%s/%d", base, PGAttribute))
	if err != nil {
		c.cache.columns[dbOID] = make(map[uint32][]AttrInfo)
		return
	}
	c.cache.columns[dbOID] = ParsePGAttribute(attrData, c.version)
}

func (c *RemoteClient) Tables(dbOID uint32) []TableInfo {
	c.loadCatalog(dbOID)
	var tables []TableInfo
	for _, t := range c.cache.tables[dbOID] {
		tables = append(tables, t)
	}
	return tables
}

func (c *RemoteClient) TablesByName(dbName string) []TableInfo {
	if db := c.Database(dbName); db != nil {
		return c.Tables(db.OID)
	}
	return nil
}

func (c *RemoteClient) Table(dbOID uint32, tableName string) *TableInfo {
	c.loadCatalog(dbOID)
	for _, t := range c.cache.tables[dbOID] {
		if strings.EqualFold(t.Name, tableName) {
			return &t
		}
	}
	return nil
}

func (c *RemoteClient) Columns(dbOID, tableOID uint32) []AttrInfo {
	c.loadCatalog(dbOID)
	return c.cache.columns[dbOID][tableOID]
}

func (c *RemoteClient) ColumnNames(dbOID, tableOID uint32) []string {
	var names []string
	for _, col := range c.Columns(dbOID, tableOID) {
		if col.Num > 0 {
			names = append(names, col.Name)
		}
	}
	return names
}

type QueryOptions struct {
	Columns []string
	Limit   int
}

func (c *RemoteClient) Query(dbOID uint32, table *TableInfo, opts *QueryOptions) []map[string]any {
	if table == nil || table.Filenode == 0 {
		return nil
	}
	data, err := c.reader(fmt.Sprintf("base/%d/%d", dbOID, table.Filenode))
	if err != nil {
		return nil
	}
	attrs := c.Columns(dbOID, table.OID)
	cols := make([]Column, len(attrs))
	for i, a := range attrs {
		cols[i] = Column{Name: a.Name, TypID: a.TypID, Len: a.Len, Num: a.Num, Align: a.Align}
	}
	rows := ReadRows(data, cols, true)
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
	if opts != nil && opts.Limit > 0 && len(rows) > opts.Limit {
		rows = rows[:opts.Limit]
	}
	return rows
}

func (c *RemoteClient) QueryByName(dbName, tableName string, opts *QueryOptions) []map[string]any {
	if db := c.Database(dbName); db != nil {
		if table := c.Table(db.OID, tableName); table != nil {
			return c.Query(db.OID, table, opts)
		}
	}
	return nil
}

func (c *RemoteClient) DumpTable(dbOID uint32, table *TableInfo) *TableDump {
	if table == nil {
		return nil
	}
	rows := c.Query(dbOID, table, nil)
	var cols []ColumnInfo
	for _, a := range c.Columns(dbOID, table.OID) {
		if a.Num > 0 {
			cols = append(cols, ColumnInfo{Name: a.Name, TypID: a.TypID, Type: TypeName(a.TypID)})
		}
	}
	return &TableDump{OID: table.OID, Name: table.Name, Filenode: table.Filenode, Kind: table.Kind, Columns: cols, Rows: rows, RowCount: len(rows)}
}

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

func (c *RemoteClient) DumpDatabaseByName(name string) *DatabaseDump {
	if db := c.Database(name); db != nil {
		return c.DumpDatabase(db.OID)
	}
	return nil
}

func (c *RemoteClient) DumpAll() *DumpResult {
	result := &DumpResult{}
	for _, db := range c.Databases() {
		if isTemplateDB(db.Name) {
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

func (c *RemoteClient) Summary() SummaryResult {
	s := SummaryResult{
		version: c.Version(),
		creds:   c.Credentials(),
		dbs:     c.Databases(),
		tables:  make(map[uint32][]TableInfo),
	}
	for _, db := range s.dbs {
		if !isTemplateDB(db.Name) {
			s.tables[db.OID] = c.Tables(db.OID)
		}
	}
	return s
}

// Exec executes a command and returns a Result (implements String() for pretty output)
func (c *RemoteClient) Exec(args []string) Result {
	cmd := ""
	if len(args) > 0 {
		cmd = args[0]
	}

	switch cmd {
	case "", "summary":
		return c.Summary()
	case "version":
		return VersionResult(c.Version())
	case "control":
		return ControlResult{c.Control()}
	case "creds", "credentials":
		return CredsResult(c.Credentials())
	case "dbs", "databases":
		return DatabasesResult(c.Databases())
	case "tables":
		if len(args) < 2 {
			return ErrorResult("usage: tables <database>")
		}
		return TablesResult(c.TablesByName(args[1]))
	case "columns":
		if len(args) < 3 {
			return ErrorResult("usage: columns <database> <table>")
		}
		db := c.Database(args[1])
		if db == nil {
			return ErrorResult("database not found")
		}
		table := c.Table(db.OID, args[2])
		if table == nil {
			return ErrorResult("table not found")
		}
		return ColumnsResult(c.Columns(db.OID, table.OID))
	case "query":
		if len(args) < 3 {
			return ErrorResult("usage: query <database> <table>")
		}
		return QueryResult(c.QueryByName(args[1], args[2], &QueryOptions{Limit: 20}))
	case "dump":
		if len(args) >= 2 {
			return DumpDatabaseResult{c.DumpDatabaseByName(args[1])}
		}
		return DumpAllResult{c.DumpAll()}
	default:
		return ErrorResult("unknown command: " + cmd)
	}
}

// Summary type for JSON
type Summary struct {
	Version     string              `json:"version,omitempty"`
	Credentials []string            `json:"credentials,omitempty"`
	Databases   map[string][]string `json:"databases,omitempty"`
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
				b.WriteString(fmt.Sprintf("%-20s", truncate(fmt.Sprintf("%v", row[col]), 20)))
			}
			b.WriteString("\n")
		}
	}
	return b.String()
}

