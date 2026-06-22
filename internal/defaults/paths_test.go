package defaults

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPathsUsesStandaloneDefaultsWhenFileIsMissing(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	defaults, err := LoadPaths()
	if err != nil {
		t.Fatal(err)
	}
	if defaults.ConfigPath != StandaloneConfigPath {
		t.Fatalf("ConfigPath = %q, want %q", defaults.ConfigPath, StandaloneConfigPath)
	}
	if defaults.DBPath != StandaloneDBPath {
		t.Fatalf("DBPath = %q, want %q", defaults.DBPath, StandaloneDBPath)
	}
	if defaults.PIDFile != StandalonePIDFile {
		t.Fatalf("PIDFile = %q, want %q", defaults.PIDFile, StandalonePIDFile)
	}
	if defaults.LogFile != StandaloneLogFile {
		t.Fatalf("LogFile = %q, want %q", defaults.LogFile, StandaloneLogFile)
	}
}

func TestLoadPathsReadsPackagedDefaults(t *testing.T) {
	defaultsFile := writeDefaultsFile(t, `
# comments are ignored
UPAG_CONFIG=/etc/upag/config.yaml
UPAG_DB='/var/lib/upag/upag.sqlite'
UPAG_PIDFILE="/run/upag/upag.pid"
UNKNOWN_KEY=ignored
`)
	withPackageDefaultsPath(t, defaultsFile)

	defaults, err := LoadPaths()
	if err != nil {
		t.Fatal(err)
	}
	if defaults.ConfigPath != "/etc/upag/config.yaml" {
		t.Fatalf("ConfigPath = %q", defaults.ConfigPath)
	}
	if defaults.DBPath != "/var/lib/upag/upag.sqlite" {
		t.Fatalf("DBPath = %q", defaults.DBPath)
	}
	if defaults.PIDFile != "/run/upag/upag.pid" {
		t.Fatalf("PIDFile = %q", defaults.PIDFile)
	}
	if defaults.LogFile != StandaloneLogFile {
		t.Fatalf("LogFile = %q, want %q", defaults.LogFile, StandaloneLogFile)
	}
}

func TestLoadPathsRejectsMalformedRelevantValue(t *testing.T) {
	defaultsFile := writeDefaultsFile(t, `UPAG_DB="/var/lib/upag/upag.sqlite`)
	withPackageDefaultsPath(t, defaultsFile)

	if _, err := LoadPaths(); err == nil {
		t.Fatal("LoadPaths returned nil error, want malformed value error")
	}
}

func TestApplyPathsFillsOnlyUnsetFlags(t *testing.T) {
	defaultsFile := writeDefaultsFile(t, `
UPAG_CONFIG=/etc/upag/config.yaml
UPAG_DB=/var/lib/upag/upag.sqlite
UPAG_PIDFILE=/run/upag/upag.pid
`)
	withPackageDefaultsPath(t, defaultsFile)

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	configPath := fs.String("config", StandaloneConfigPath, "")
	dbPath := fs.String("db", StandaloneDBPath, "")
	pidFile := fs.String("pid-file", StandalonePIDFile, "")
	if err := fs.Parse([]string{"--db", "/tmp/explicit.sqlite"}); err != nil {
		t.Fatal(err)
	}

	err := ApplyPaths(fs,
		PathTarget{FlagName: "config", Value: configPath, Default: func(d Paths) string { return d.ConfigPath }},
		PathTarget{FlagName: "db", Value: dbPath, Default: func(d Paths) string { return d.DBPath }},
		PathTarget{FlagName: "pid-file", Value: pidFile, Default: func(d Paths) string { return d.PIDFile }},
	)
	if err != nil {
		t.Fatal(err)
	}
	if *configPath != "/etc/upag/config.yaml" {
		t.Fatalf("configPath = %q", *configPath)
	}
	if *dbPath != "/tmp/explicit.sqlite" {
		t.Fatalf("dbPath = %q, want explicit flag value", *dbPath)
	}
	if *pidFile != "/run/upag/upag.pid" {
		t.Fatalf("pidFile = %q", *pidFile)
	}
}

func withPackageDefaultsPath(t *testing.T, path string) {
	t.Helper()
	previous := packageDefaultsPath
	packageDefaultsPath = path
	t.Cleanup(func() {
		packageDefaultsPath = previous
	})
}

func writeDefaultsFile(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "upag.default")
	if err := os.WriteFile(path, []byte(contents), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
