package pgdump

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// TableFormat writes dump results as psql-style formatted tables
func (r *DumpResult) TableFormat(w io.Writer) {
	for _, db := range r.Databases {
		for _, t := range db.Tables {
			if t.RowCount == 0 {
				continue
			}
			writeTable(w, db.Name, t)
		}
	}
}

func writeTable(w io.Writer, dbName string, t TableDump) {
	if len(t.Columns) == 0 || len(t.Rows) == 0 {
		return
	}

	// Collect column names
	colNames := make([]string, len(t.Columns))
	for i, c := range t.Columns {
		colNames[i] = c.Name
	}

	// Calculate max width for each column
	widths := make([]int, len(colNames))
	for i, name := range colNames {
		widths[i] = len(name)
	}
	for _, row := range t.Rows {
		for i, name := range colNames {
			val := formatCell(row[name])
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	// Cap column width
	maxWidth := 60
	for i := range widths {
		if widths[i] > maxWidth {
			widths[i] = maxWidth
		}
	}

	// Print header
	fmt.Fprintf(w, "\n %s.%s (%d rows)\n", dbName, t.Name, t.RowCount)

	tw := tabwriter.NewWriter(w, 0, 0, 1, ' ', 0)

	// Column names
	var header []string
	var separator []string
	for i, name := range colNames {
		header = append(header, fmt.Sprintf(" %-*s", widths[i], truncateCell(name, widths[i])))
		separator = append(separator, strings.Repeat("-", widths[i]+1))
	}
	fmt.Fprintln(tw, strings.Join(header, " |"))
	fmt.Fprintln(tw, strings.Join(separator, "-+"))

	// Rows
	for _, row := range t.Rows {
		var cells []string
		for i, name := range colNames {
			val := formatCell(row[name])
			cells = append(cells, fmt.Sprintf(" %-*s", widths[i], truncateCell(val, widths[i])))
		}
		fmt.Fprintln(tw, strings.Join(cells, " |"))
	}

	tw.Flush()
	fmt.Fprintf(w, "(%d rows)\n\n", t.RowCount)
}

func formatCell(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	s := fmt.Sprintf("%v", v)
	// Replace newlines and tabs for table display
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\t", "\\t")
	s = strings.ReplaceAll(s, "\r", "")
	return s
}

func truncateCell(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}
