package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPathDefaultsUsesStandaloneDefaultsWhenFileIsMissing(t *testing.T) {
	withPackageDefaultsPath(t, filepath.Join(t.TempDir(), "missing"))

	defaults, err := loadPathDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if defaults.ConfigPath != standaloneConfigPath {
		t.Fatalf("ConfigPath = %q, want %q", defaults.ConfigPath, standaloneConfigPath)
	}
	if defaults.DBPath != standaloneDBPath {
		t.Fatalf("DBPath = %q, want %q", defaults.DBPath, standaloneDBPath)
	}
	if defaults.PIDFile != standalonePIDFile {
		t.Fatalf("PIDFile = %q, want %q", defaults.PIDFile, standalonePIDFile)
	}
	if defaults.LogFile != standaloneLogFile {
		t.Fatalf("LogFile = %q, want %q", defaults.LogFile, standaloneLogFile)
	}
}

func TestLoadPathDefaultsReadsPackagedDefaults(t *testing.T) {
	defaultsFile := writeDefaultsFile(t, `
# comments are ignored
UPAG_CONFIG=/etc/upag/config.yaml
UPAG_DB='/var/lib/upag/upag.sqlite'
UPAG_PIDFILE="/run/upag/upag.pid"
UNKNOWN_KEY=ignored
`)
	withPackageDefaultsPath(t, defaultsFile)

	defaults, err := loadPathDefaults()
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
	if defaults.LogFile != standaloneLogFile {
		t.Fatalf("LogFile = %q, want %q", defaults.LogFile, standaloneLogFile)
	}
}

func TestLoadPathDefaultsRejectsMalformedRelevantValue(t *testing.T) {
	defaultsFile := writeDefaultsFile(t, `UPAG_DB="/var/lib/upag/upag.sqlite`)
	withPackageDefaultsPath(t, defaultsFile)

	if _, err := loadPathDefaults(); err == nil {
		t.Fatal("loadPathDefaults returned nil error, want malformed value error")
	}
}

func TestApplyPathDefaultsFillsOnlyUnsetFlags(t *testing.T) {
	defaultsFile := writeDefaultsFile(t, `
UPAG_CONFIG=/etc/upag/config.yaml
UPAG_DB=/var/lib/upag/upag.sqlite
UPAG_PIDFILE=/run/upag/upag.pid
`)
	withPackageDefaultsPath(t, defaultsFile)

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	configPath := fs.String("config", standaloneConfigPath, "")
	dbPath := fs.String("db", standaloneDBPath, "")
	pidFile := fs.String("pid-file", standalonePIDFile, "")
	if err := fs.Parse([]string{"--db", "/tmp/explicit.sqlite"}); err != nil {
		t.Fatal(err)
	}

	err := applyPathDefaults(fs,
		pathDefaultTarget{FlagName: "config", Value: configPath, Default: func(d pathDefaults) string { return d.ConfigPath }},
		pathDefaultTarget{FlagName: "db", Value: dbPath, Default: func(d pathDefaults) string { return d.DBPath }},
		pathDefaultTarget{FlagName: "pid-file", Value: pidFile, Default: func(d pathDefaults) string { return d.PIDFile }},
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
