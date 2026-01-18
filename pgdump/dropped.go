package pgdump

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// DroppedColumnInfo contains information about a dropped column
type DroppedColumnInfo struct {
	RelOID       uint32 `json:"rel_oid"`
	TableName    string `json:"table_name,omitempty"`
	AttNum       int    `json:"attnum"`
	OriginalName string `json:"original_name,omitempty"`  // If recoverable
	DroppedName  string `json:"dropped_name"`            // pg.dropped.N format
	TypeOID      uint32 `json:"type_oid"`
	TypeName     string `json:"type_name"`
	AttLen       int    `json:"attlen"`
	AttAlign     byte   `json:"attalign"`
	AttByVal     bool   `json:"attbyval"`
}

// DroppedColumnData contains recovered data from a dropped column
type DroppedColumnData struct {
	Column DroppedColumnInfo        `json:"column"`
	Values []interface{}            `json:"values,omitempty"`
	Rows   []map[string]interface{} `json:"rows,omitempty"` // Full rows if requested
}

// DroppedColumnsResult contains all dropped columns info for a database
type DroppedColumnsResult struct {
	Database     string              `json:"database"`
	DroppedCount int                 `json:"dropped_count"`
	Columns      []DroppedColumnInfo `json:"columns"`
}

// Extended pg_attribute schema that includes attisdropped
var schemaPGAttrDropped = []Column{
	{Name: "attrelid", TypID: OidOid, Len: 4},
	{Name: "attname", TypID: OidName, Len: 64},
	{Name: "atttypid", TypID: OidOid, Len: 4},
	{Name: "attlen", TypID: OidInt2, Len: 2},
	{Name: "attnum", TypID: OidInt2, Len: 2},
	{Name: "atttypmod", TypID: OidInt4, Len: 4},
	{Name: "attndims", TypID: OidInt2, Len: 2},
	{Name: "attbyval", TypID: OidBool, Len: 1},
	{Name: "attstorage", TypID: OidChar, Len: 1},
	{Name: "attalign", TypID: OidChar, Len: 1},
	{Name: "attnotnull", TypID: OidBool, Len: 1},
	{Name: "atthasdef", TypID: OidBool, Len: 1},
	{Name: "atthasmissing", TypID: OidBool, Len: 1},
	{Name: "attidentity", TypID: OidChar, Len: 1},
	{Name: "attgenerated", TypID: OidChar, Len: 1},
	{Name: "attisdropped", TypID: OidBool, Len: 1},
}

// V15 schema with attstattarget
var schemaPGAttrDroppedV15 = []Column{
	{Name: "attrelid", TypID: OidOid, Len: 4},
	{Name: "attname", TypID: OidName, Len: 64},
	{Name: "atttypid", TypID: OidOid, Len: 4},
	{Name: "attstattarget", TypID: OidInt4, Len: 4},
	{Name: "attlen", TypID: OidInt2, Len: 2},
	{Name: "attnum", TypID: OidInt2, Len: 2},
	{Name: "atttypmod", TypID: OidInt4, Len: 4},
	{Name: "attndims", TypID: OidInt2, Len: 2},
	{Name: "attbyval", TypID: OidBool, Len: 1},
	{Name: "attstorage", TypID: OidChar, Len: 1},
	{Name: "attalign", TypID: OidChar, Len: 1},
	{Name: "attnotnull", TypID: OidBool, Len: 1},
	{Name: "atthasdef", TypID: OidBool, Len: 1},
	{Name: "atthasmissing", TypID: OidBool, Len: 1},
	{Name: "attidentity", TypID: OidChar, Len: 1},
	{Name: "attgenerated", TypID: OidChar, Len: 1},
	{Name: "attisdropped", TypID: OidBool, Len: 1},
}

// droppedColumnRegex matches PostgreSQL's dropped column naming pattern
var droppedColumnRegex = regexp.MustCompile(`^\.+pg\.dropped\.(\d+)\.+$`)

// FindDroppedColumns finds all dropped columns in a database
func FindDroppedColumns(dataDir, dbName string) (*DroppedColumnsResult, error) {
	result := &DroppedColumnsResult{
		Database: dbName,
	}
	
	// First, find the database OID
	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, fmt.Errorf("cannot read pg_database: %w", err)
	}
	
	var dbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == dbName {
			dbOID = db.OID
			break
		}
	}
	if dbOID == 0 {
		return nil, fmt.Errorf("database %q not found", dbName)
	}
	
	basePath := filepath.Join(dataDir, "base", strconv.FormatUint(uint64(dbOID), 10))
	
	// Read pg_attribute
	attrData, err := os.ReadFile(filepath.Join(basePath, "1249"))
	if err != nil {
		return nil, fmt.Errorf("cannot read pg_attribute: %w", err)
	}
	
	// Read pg_class for table names
	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		return nil, fmt.Errorf("cannot read pg_class: %w", err)
	}
	
	tables := ParsePGClass(classData)
	tableNames := make(map[uint32]string)
	for _, t := range tables {
		tableNames[t.OID] = t.Name
	}
	
	// Parse attributes looking for dropped columns
	droppedCols := parseDroppedColumns(attrData, tableNames)
	
	result.Columns = droppedCols
	result.DroppedCount = len(droppedCols)
	
	return result, nil
}

// parseDroppedColumns parses pg_attribute looking for dropped columns
func parseDroppedColumns(data []byte, tableNames map[uint32]string) []DroppedColumnInfo {
	var dropped []DroppedColumnInfo
	
	// Try V16 schema first
	rows := ReadRows(data, schemaPGAttrDropped, true)
	if len(rows) == 0 {
		// Try V15 schema
		rows = ReadRows(data, schemaPGAttrDroppedV15, true)
	}
	
	for _, row := range rows {
		// Check if column is dropped
		isDrop, ok := row["attisdropped"].(bool)
		if !ok || !isDrop {
			continue
		}
		
		attnum := toInt(row["attnum"])
		if attnum <= 0 {
			continue
		}
		
		relid := getOID(row, "attrelid")
		attname := getString(row, "attname")
		
		col := DroppedColumnInfo{
			RelOID:      relid,
			TableName:   tableNames[relid],
			AttNum:      attnum,
			DroppedName: attname,
			TypeOID:     getOID(row, "atttypid"),
			AttLen:      toInt(row["attlen"]),
		}
		
		// Get type name
		col.TypeName = TypeName(int(col.TypeOID))
		
		// Get alignment
		if align := getString(row, "attalign"); len(align) > 0 {
			col.AttAlign = align[0]
		}
		
		// Get byval
		if byval, ok := row["attbyval"].(bool); ok {
			col.AttByVal = byval
		}
		
		// Try to extract original column number from dropped name
		if matches := droppedColumnRegex.FindStringSubmatch(attname); len(matches) > 1 {
			// The number in the dropped name is usually the original attnum
			col.OriginalName = fmt.Sprintf("dropped_%s", matches[1])
		}
		
		dropped = append(dropped, col)
	}
	
	// Sort by relid, then attnum
	sort.Slice(dropped, func(i, j int) bool {
		if dropped[i].RelOID != dropped[j].RelOID {
			return dropped[i].RelOID < dropped[j].RelOID
		}
		return dropped[i].AttNum < dropped[j].AttNum
	})
	
	return dropped
}

// RecoverDroppedColumnData attempts to recover data from a dropped column
func RecoverDroppedColumnData(dataDir, dbName, tableName string, attNum int) (*DroppedColumnData, error) {
	// Find database OID
	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, err
	}
	
	var dbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == dbName {
			dbOID = db.OID
			break
		}
	}
	if dbOID == 0 {
		return nil, fmt.Errorf("database %q not found", dbName)
	}
	
	basePath := filepath.Join(dataDir, "base", strconv.FormatUint(uint64(dbOID), 10))
	
	// Find table filenode
	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		return nil, err
	}
	
	tables := ParsePGClass(classData)
	var tableInfo *TableInfo
	for _, t := range tables {
		if t.Name == tableName {
			info := t
			tableInfo = &info
			break
		}
	}
	if tableInfo == nil {
		return nil, fmt.Errorf("table %q not found", tableName)
	}
	
	// Get all attributes including dropped ones
	attrData, err := os.ReadFile(filepath.Join(basePath, "1249"))
	if err != nil {
		return nil, err
	}
	
	// Get full attribute info with dropped columns
	allAttrs := parseAllAttributes(attrData, tableInfo.OID)
	
	// Find the dropped column
	var droppedCol *DroppedColumnInfo
	for _, col := range allAttrs {
		if col.AttNum == attNum {
			droppedCol = &col
			break
		}
	}
	if droppedCol == nil {
		return nil, fmt.Errorf("column attnum %d not found", attNum)
	}
	
	result := &DroppedColumnData{
		Column: *droppedCol,
	}
	
	// Read table data
	tableData, err := os.ReadFile(filepath.Join(basePath, strconv.FormatUint(uint64(tableInfo.Filenode), 10)))
	if err != nil {
		return nil, fmt.Errorf("cannot read table data: %w", err)
	}
	
	// Build column schema including dropped columns
	cols := buildColumnsWithDropped(allAttrs)
	
	// Read rows
	rows := ReadRows(tableData, cols, true)
	
	// Extract dropped column values
	droppedName := fmt.Sprintf("dropped_%d", attNum)
	for _, row := range rows {
		if val, ok := row[droppedName]; ok {
			result.Values = append(result.Values, val)
		} else {
			result.Values = append(result.Values, nil)
		}
	}
	
	// Optionally include full rows
	result.Rows = rows
	
	return result, nil
}

// parseAllAttributes parses pg_attribute including dropped columns for a specific table
func parseAllAttributes(data []byte, relOID uint32) []DroppedColumnInfo {
	var attrs []DroppedColumnInfo
	
	// Try V16 schema first
	rows := ReadRows(data, schemaPGAttrDropped, true)
	if len(rows) == 0 {
		rows = ReadRows(data, schemaPGAttrDroppedV15, true)
	}
	
	for _, row := range rows {
		relid := getOID(row, "attrelid")
		if relid != relOID {
			continue
		}
		
		attnum := toInt(row["attnum"])
		if attnum <= 0 {
			continue
		}
		
		isDrop, _ := row["attisdropped"].(bool)
		attname := getString(row, "attname")
		
		col := DroppedColumnInfo{
			RelOID:      relid,
			AttNum:      attnum,
			DroppedName: attname,
			TypeOID:     getOID(row, "atttypid"),
			AttLen:      toInt(row["attlen"]),
		}
		
		if isDrop {
			col.OriginalName = fmt.Sprintf("dropped_%d", attnum)
		} else {
			col.OriginalName = attname
		}
		
		col.TypeName = TypeName(int(col.TypeOID))
		
		if align := getString(row, "attalign"); len(align) > 0 {
			col.AttAlign = align[0]
		}
		
		if byval, ok := row["attbyval"].(bool); ok {
			col.AttByVal = byval
		}
		
		attrs = append(attrs, col)
	}
	
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].AttNum < attrs[j].AttNum
	})
	
	return attrs
}

// buildColumnsWithDropped builds Column slice including dropped columns
func buildColumnsWithDropped(attrs []DroppedColumnInfo) []Column {
	cols := make([]Column, 0, len(attrs))
	
	for _, a := range attrs {
		name := a.OriginalName
		if name == "" {
			name = fmt.Sprintf("dropped_%d", a.AttNum)
		}
		
		cols = append(cols, Column{
			Name:  name,
			TypID: int(a.TypeOID),
			Len:   a.AttLen,
			Num:   a.AttNum,
			Align: a.AttAlign,
		})
	}
	
	return cols
}

// ScanDroppedColumns scans all databases for dropped columns
func ScanDroppedColumns(dataDir string) ([]DroppedColumnsResult, error) {
	var results []DroppedColumnsResult
	
	// Read database list
	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, err
	}
	
	for _, db := range ParsePGDatabase(dbData) {
		if strings.HasPrefix(db.Name, "template") {
			continue
		}
		
		result, err := FindDroppedColumns(dataDir, db.Name)
		if err != nil {
			continue
		}
		
		if result.DroppedCount > 0 {
			results = append(results, *result)
		}
	}
	
	return results, nil
}

// GetDroppedColumnSchema returns a schema that includes dropped columns for a table
func GetDroppedColumnSchema(dataDir, dbName, tableName string) ([]Column, error) {
	dbData, err := os.ReadFile(filepath.Join(dataDir, "global", "1262"))
	if err != nil {
		return nil, err
	}
	
	var dbOID uint32
	for _, db := range ParsePGDatabase(dbData) {
		if db.Name == dbName {
			dbOID = db.OID
			break
		}
	}
	if dbOID == 0 {
		return nil, fmt.Errorf("database %q not found", dbName)
	}
	
	basePath := filepath.Join(dataDir, "base", strconv.FormatUint(uint64(dbOID), 10))
	
	classData, err := os.ReadFile(filepath.Join(basePath, "1259"))
	if err != nil {
		return nil, err
	}
	
	tables := ParsePGClass(classData)
	var tableOID uint32
	for _, t := range tables {
		if t.Name == tableName {
			tableOID = t.OID
			break
		}
	}
	if tableOID == 0 {
		return nil, fmt.Errorf("table %q not found", tableName)
	}
	
	attrData, err := os.ReadFile(filepath.Join(basePath, "1249"))
	if err != nil {
		return nil, err
	}
	
	attrs := parseAllAttributes(attrData, tableOID)
	return buildColumnsWithDropped(attrs), nil
}
