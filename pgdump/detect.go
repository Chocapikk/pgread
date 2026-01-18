package pgdump

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// DetectDataDir attempts to find PostgreSQL data directory on the system
func DetectDataDir() string {
	// Check environment variable first
	if pgdata := os.Getenv("PGDATA"); pgdata != "" {
		if isValidDataDir(pgdata) {
			return pgdata
		}
	}

	// Get platform-specific paths
	candidates := getDataDirCandidates()

	// Check each candidate
	for _, path := range candidates {
		if isValidDataDir(path) {
			return path
		}
	}

	return ""
}

// DetectAllDataDirs finds all PostgreSQL data directories on the system
func DetectAllDataDirs() []string {
	seen := make(map[string]bool)
	var results []string

	// Check PGDATA first
	if pgdata := os.Getenv("PGDATA"); pgdata != "" && isValidDataDir(pgdata) {
		results = append(results, pgdata)
		seen[pgdata] = true
	}

	// Check all candidates
	for _, path := range getDataDirCandidates() {
		resolved := expandPath(path)
		if seen[resolved] {
			continue
		}
		if isValidDataDir(resolved) {
			results = append(results, resolved)
			seen[resolved] = true
		}
	}

	return results
}

func getDataDirCandidates() []string {
	switch runtime.GOOS {
	case "linux":
		return getLinuxPaths()
	case "darwin":
		return getDarwinPaths()
	case "windows":
		return getWindowsPaths()
	default:
		return getLinuxPaths() // Fallback
	}
}

func getLinuxPaths() []string {
	paths := []string{
		// Standard paths
		"/var/lib/postgresql/data",
		"/var/lib/pgsql/data",
		// Docker default
		"/var/lib/postgresql/data",
	}

	// Debian/Ubuntu versioned paths
	for v := 17; v >= 10; v-- {
		paths = append(paths, "/var/lib/postgresql/"+strconv.Itoa(v)+"/main")
	}

	// RHEL/CentOS versioned paths
	for v := 17; v >= 10; v-- {
		paths = append(paths, "/var/lib/pgsql/"+strconv.Itoa(v)+"/data")
	}

	// Common custom paths
	paths = append(paths,
		"/opt/postgresql/data",
		"/data/postgresql",
		"/pgdata",
	)

	return paths
}

func getDarwinPaths() []string {
	home, _ := os.UserHomeDir()
	paths := []string{
		// Homebrew Intel
		"/usr/local/var/postgres",
		"/usr/local/var/postgresql",
		// Homebrew Apple Silicon
		"/opt/homebrew/var/postgres",
		"/opt/homebrew/var/postgresql",
		// Postgres.app
		home + "/Library/Application Support/Postgres/var-17",
		home + "/Library/Application Support/Postgres/var-16",
		home + "/Library/Application Support/Postgres/var-15",
		// Official installer
		"/Library/PostgreSQL/17/data",
		"/Library/PostgreSQL/16/data",
		"/Library/PostgreSQL/15/data",
		"/Library/PostgreSQL/14/data",
	}

	// Homebrew versioned
	for v := 17; v >= 12; v-- {
		paths = append(paths,
			"/usr/local/var/postgresql@"+strconv.Itoa(v),
			"/opt/homebrew/var/postgresql@"+strconv.Itoa(v),
		)
	}

	return paths
}

func getWindowsPaths() []string {
	var paths []string

	// Standard install paths
	progFiles := os.Getenv("ProgramFiles")
	if progFiles == "" {
		progFiles = "C:\\Program Files"
	}
	progData := os.Getenv("ProgramData")
	if progData == "" {
		progData = "C:\\ProgramData"
	}

	for v := 17; v >= 10; v-- {
		vs := strconv.Itoa(v)
		paths = append(paths,
			filepath.Join(progFiles, "PostgreSQL", vs, "data"),
			filepath.Join(progData, "PostgreSQL", vs, "data"),
		)
	}

	// EnterpriseDB paths
	paths = append(paths,
		filepath.Join(progFiles, "edb", "as17", "data"),
		filepath.Join(progFiles, "edb", "as16", "data"),
	)

	return paths
}

func isValidDataDir(path string) bool {
	// Must have global/1262 (pg_database)
	pgDatabase := filepath.Join(path, "global", "1262")
	info, err := os.Stat(pgDatabase)
	if err != nil {
		return false
	}
	// Should be a file with some content
	return info.Mode().IsRegular() && info.Size() > 0
}

func expandPath(path string) string {
	if strings.HasPrefix(path, "~") {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, path[1:])
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	return abs
}

// ListDatabases returns databases found in data directory (quick scan)
func ListDatabases(dataDir string) []DatabaseInfo {
	pgDatabase := filepath.Join(dataDir, "global", "1262")
	data, err := os.ReadFile(pgDatabase)
	if err != nil {
		return nil
	}
	
	dbs := ParsePGDatabase(data)
	
	// Sort by name, templates last
	sort.Slice(dbs, func(i, j int) bool {
		iTemplate := strings.HasPrefix(dbs[i].Name, "template")
		jTemplate := strings.HasPrefix(dbs[j].Name, "template")
		if iTemplate != jTemplate {
			return !iTemplate
		}
		return dbs[i].Name < dbs[j].Name
	})
	
	return dbs
}
