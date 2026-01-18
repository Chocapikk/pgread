package pgdump

import (
	"fmt"
	"regexp"
)

// SearchResult represents a match found during search
type SearchResult struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Column   string                 `json:"column"`
	RowNum   int                    `json:"row_num"`
	Value    interface{}            `json:"value"`
	Row      map[string]interface{} `json:"row,omitempty"`
}

// SearchOptions configures the search behavior
type SearchOptions struct {
	Pattern       string // Regex pattern to search for
	CaseSensitive bool   // Case-sensitive search
	IncludeRow    bool   // Include full row in results
	MaxResults    int    // Maximum results (0 = unlimited)
}

// Search searches across all databases and tables for a pattern
func Search(dataDir string, opts *SearchOptions) ([]SearchResult, error) {
	if opts == nil {
		return nil, fmt.Errorf("search options required")
	}

	// Compile regex
	pattern := opts.Pattern
	if !opts.CaseSensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}

	// Dump everything
	result, err := DumpDataDir(dataDir, &Options{SkipSystemTables: true})
	if err != nil {
		return nil, err
	}

	var matches []SearchResult

	for _, db := range result.Databases {
		for _, table := range db.Tables {
			for rowNum, row := range table.Rows {
				for colName, value := range row {
					if matchValue(value, re) {
						match := SearchResult{
							Database: db.Name,
							Table:    table.Name,
							Column:   colName,
							RowNum:   rowNum,
							Value:    value,
						}
						if opts.IncludeRow {
							match.Row = row
						}
						matches = append(matches, match)

						if opts.MaxResults > 0 && len(matches) >= opts.MaxResults {
							return matches, nil
						}
					}
				}
			}
		}
	}

	return matches, nil
}

// SearchInDump searches within an already-loaded dump result
func SearchInDump(result *DumpResult, opts *SearchOptions) ([]SearchResult, error) {
	if opts == nil {
		return nil, fmt.Errorf("search options required")
	}

	pattern := opts.Pattern
	if !opts.CaseSensitive {
		pattern = "(?i)" + pattern
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern: %w", err)
	}

	var matches []SearchResult

	for _, db := range result.Databases {
		for _, table := range db.Tables {
			for rowNum, row := range table.Rows {
				for colName, value := range row {
					if matchValue(value, re) {
						match := SearchResult{
							Database: db.Name,
							Table:    table.Name,
							Column:   colName,
							RowNum:   rowNum,
							Value:    value,
						}
						if opts.IncludeRow {
							match.Row = row
						}
						matches = append(matches, match)

						if opts.MaxResults > 0 && len(matches) >= opts.MaxResults {
							return matches, nil
						}
					}
				}
			}
		}
	}

	return matches, nil
}

// matchValue checks if a value matches the regex
func matchValue(value interface{}, re *regexp.Regexp) bool {
	if value == nil {
		return false
	}

	switch v := value.(type) {
	case string:
		return re.MatchString(v)
	case []byte:
		return re.Match(v)
	case map[string]interface{}:
		// Search in JSON/JSONB
		return matchMap(v, re)
	case []interface{}:
		// Search in arrays
		for _, elem := range v {
			if matchValue(elem, re) {
				return true
			}
		}
	default:
		// Convert to string and search
		str := fmt.Sprintf("%v", v)
		return re.MatchString(str)
	}
	return false
}

// matchMap recursively searches in a map
func matchMap(m map[string]interface{}, re *regexp.Regexp) bool {
	for key, val := range m {
		if re.MatchString(key) {
			return true
		}
		if matchValue(val, re) {
			return true
		}
	}
	return false
}

// QuickSearch is a convenience function for simple string search
func QuickSearch(dataDir, pattern string) ([]SearchResult, error) {
	return Search(dataDir, &SearchOptions{
		Pattern:       regexp.QuoteMeta(pattern),
		CaseSensitive: false,
		IncludeRow:    true,
	})
}

// SearchSecrets searches for secrets using trufflehog detectors
// This is a convenience wrapper around ScanForSecrets
// Deprecated: Use ScanForSecrets instead for more accurate detection
func SearchSecrets(dataDir string) ([]SearchResult, error) {
	findings, err := ScanForSecrets(dataDir, &Options{SkipSystemTables: true})
	if err != nil {
		return nil, err
	}

	// Convert SecretFinding to SearchResult for backwards compatibility
	var results []SearchResult
	for _, f := range findings {
		results = append(results, SearchResult{
			Database: f.Database,
			Table:    f.Table,
			Column:   f.Column,
			RowNum:   f.RowIndex,
			Value:    f.Raw,
			Row: map[string]interface{}{
				"detector": f.DetectorName,
				"redacted": f.Redacted,
				"verified": f.Verified,
			},
		})
	}
	return results, nil
}
