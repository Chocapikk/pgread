// Package pgdump provides PostgreSQL data directory parsing capabilities.
// This file implements secret detection using trufflehog detectors.
package pgdump

import (
	"context"
	"fmt"
	"strings"

	"github.com/trufflesecurity/trufflehog/v3/pkg/detectors"
	"github.com/trufflesecurity/trufflehog/v3/pkg/engine/defaults"
)

// SecretFinding represents a detected secret
type SecretFinding struct {
	DetectorName string            `json:"detector"`
	Database     string            `json:"database"`
	Table        string            `json:"table"`
	Column       string            `json:"column,omitempty"`
	RowIndex     int               `json:"row_index"`
	Raw          string            `json:"raw"`
	Redacted     string            `json:"redacted,omitempty"`
	Verified     bool              `json:"verified"`
	ExtraData    map[string]string `json:"extra_data,omitempty"`
}

// SecretScanner scans for secrets using trufflehog detectors
type SecretScanner struct {
	detectors []detectors.Detector
}

// NewSecretScanner creates a new scanner with all trufflehog detectors
func NewSecretScanner() *SecretScanner {
	return &SecretScanner{
		detectors: defaults.DefaultDetectors(),
	}
}

// ScanString scans a string for secrets without verification
func (s *SecretScanner) ScanString(data string) []detectors.Result {
	var results []detectors.Result
	ctx := context.Background()
	dataBytes := []byte(data)

	for _, detector := range s.detectors {
		// Check keywords first for efficiency
		// Keywords can be case-sensitive or case-insensitive depending on detector
		keywords := detector.Keywords()
		hasKeyword := false
		for _, kw := range keywords {
			// Check both exact match and case-insensitive match
			if strings.Contains(data, kw) || containsIgnoreCase(data, kw) {
				hasKeyword = true
				break
			}
		}
		if !hasKeyword && len(keywords) > 0 {
			continue
		}

		// Run detector without verification
		found, err := detector.FromData(ctx, false, dataBytes)
		if err != nil {
			continue
		}
		results = append(results, found...)
	}

	return results
}

// ScanDumpResult scans a DumpResult for secrets
func (s *SecretScanner) ScanDumpResult(result *DumpResult) []SecretFinding {
	var findings []SecretFinding

	for _, db := range result.Databases {
		dbFindings := s.ScanDatabaseDump(&db)
		findings = append(findings, dbFindings...)
	}

	return findings
}

// ScanDatabaseDump scans a DatabaseDump for secrets
func (s *SecretScanner) ScanDatabaseDump(db *DatabaseDump) []SecretFinding {
	var findings []SecretFinding

	for _, table := range db.Tables {
		tableFindings := s.scanTable(db.Name, &table)
		findings = append(findings, tableFindings...)
	}

	return findings
}

func (s *SecretScanner) scanTable(dbName string, table *TableDump) []SecretFinding {
	var findings []SecretFinding

	for rowIdx, row := range table.Rows {
		for colName, value := range row {
			strVal := fmt.Sprintf("%v", value)
			if len(strVal) < 8 {
				continue // Too short to be a secret
			}

			results := s.ScanString(strVal)
			for _, res := range results {
				finding := SecretFinding{
					DetectorName: res.DetectorType.String(),
					Database:     dbName,
					Table:        table.Name,
					Column:       colName,
					RowIndex:     rowIdx,
					Raw:          string(res.Raw),
					Redacted:     res.Redacted,
					Verified:     res.Verified,
					ExtraData:    res.ExtraData,
				}
				findings = append(findings, finding)
			}
		}
	}

	return findings
}

// ScanDataDir scans a PostgreSQL data directory for secrets
func ScanForSecrets(dataDir string, opts *Options) ([]SecretFinding, error) {
	result, err := DumpDataDir(dataDir, opts)
	if err != nil {
		return nil, err
	}

	scanner := NewSecretScanner()
	return scanner.ScanDumpResult(result), nil
}

// containsIgnoreCase checks if s contains substr (case-insensitive)
func containsIgnoreCase(s, substr string) bool {
	sLower := make([]byte, len(s))
	substrLower := make([]byte, len(substr))

	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			sLower[i] = c + 32
		} else {
			sLower[i] = c
		}
	}

	for i := 0; i < len(substr); i++ {
		c := substr[i]
		if c >= 'A' && c <= 'Z' {
			substrLower[i] = c + 32
		} else {
			substrLower[i] = c
		}
	}

	return bytesContains(sLower, substrLower)
}

func bytesContains(s, substr []byte) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if bytesEqual(s[i:i+len(substr)], substr) {
			return true
		}
	}
	return false
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
