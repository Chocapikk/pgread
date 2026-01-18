package pgdump

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"testing"
)

// generateRandomHex generates a random hex string of given length
func generateRandomHex(length int) string {
	bytes := make([]byte, length/2)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// generateTestToken creates a test token with the given prefix and random suffix
func generateTestToken(prefix string, suffixLen int) string {
	return prefix + generateRandomHex(suffixLen)
}

func TestSecretScannerDetection(t *testing.T) {
	scanner := NewSecretScanner()

	// Generate test tokens dynamically to avoid GitHub push protection
	// These are structurally valid but random tokens
	tests := []struct {
		name     string
		input    string
		wantFind bool
	}{
		{
			name:     "Stripe Live Key",
			input:    generateTestToken("sk_live_51", 40),
			wantFind: true,
		},
		{
			name:     "Slack Bot Token",
			input:    fmt.Sprintf("xoxb-%d-%d-%s", 41521398724+mrand.Int63()%1000, 8174928371924+mrand.Int63()%1000, generateRandomHex(24)),
			wantFind: true,
		},
		{
			name:     "Regular text",
			input:    "Hello, this is just regular text without any secrets",
			wantFind: false,
		},
		{
			name:     "Short string",
			input:    "abc",
			wantFind: false,
		},
		{
			name:     "GitLab PAT",
			input:    generateTestToken("glpat-", 20),
			wantFind: true,
		},
		{
			name:     "DigitalOcean PAT",
			input:    generateTestToken("dop_v1_", 64),
			wantFind: true,
		},
		{
			name:     "Doppler Token",
			input:    generateTestToken("dp.pt.", 40),
			wantFind: true,
		},
		{
			name:     "SendGrid API Key",
			input:    fmt.Sprintf("SG.%s.%s", generateRandomHex(22), generateRandomHex(43)),
			wantFind: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := scanner.ScanString(tt.input)
			found := len(results) > 0

			if found != tt.wantFind {
				if tt.wantFind {
					t.Errorf("Expected to find secret in pattern but didn't")
				} else {
					t.Errorf("Did not expect to find secret but found %d", len(results))
				}
			}

			if found && tt.wantFind {
				t.Logf("Found %d secret(s) of type: %s", len(results), results[0].DetectorType.String())
			}
		})
	}
}

func TestSecretScannerWithDump(t *testing.T) {
	scanner := NewSecretScanner()

	// Create mock dump result with dynamically generated tokens
	result := &DumpResult{
		Databases: []DatabaseDump{
			{
				OID:  1,
				Name: "testdb",
				Tables: []TableDump{
					{
						Name: "credentials",
						Rows: []map[string]interface{}{
							{"key": "stripe", "value": generateTestToken("sk_live_51", 40)},
							{"key": "slack", "value": fmt.Sprintf("xoxb-%d-%d-%s", 41521398724+mrand.Int63()%1000, 8174928371924+mrand.Int63()%1000, generateRandomHex(24))},
							{"key": "gitlab", "value": generateTestToken("glpat-", 20)},
							{"key": "normal", "value": "just some text without secrets"},
						},
					},
				},
			},
		},
	}

	findings := scanner.ScanDumpResult(result)

	if len(findings) < 2 {
		t.Errorf("Expected at least 2 findings, got %d", len(findings))
		for _, f := range findings {
			t.Logf("Found: %s", f.DetectorName)
		}
	}

	// Check that findings have correct metadata
	for _, f := range findings {
		if f.Database != "testdb" {
			t.Errorf("Expected database 'testdb', got %s", f.Database)
		}
		if f.Table != "credentials" {
			t.Errorf("Expected table 'credentials', got %s", f.Table)
		}
		t.Logf("Found: %s in %s.%s.%s (row %d)", f.DetectorName, f.Database, f.Table, f.Column, f.RowIndex)
	}
}

func TestContainsIgnoreCase(t *testing.T) {
	tests := []struct {
		s      string
		substr string
		want   bool
	}{
		{"Hello World", "world", true},
		{"Hello World", "WORLD", true},
		{"Hello World", "foo", false},
		{"AKIA12345", "akia", true},
		{"ghp_token", "GHP", true},
		{"", "test", false},
		{"test", "", true},
	}

	for _, tt := range tests {
		got := containsIgnoreCase(tt.s, tt.substr)
		if got != tt.want {
			t.Errorf("containsIgnoreCase(%q, %q) = %v, want %v", tt.s, tt.substr, got, tt.want)
		}
	}
}
