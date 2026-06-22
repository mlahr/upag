package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
)

const (
	standaloneConfigPath = "./config.yaml"
	standaloneDBPath     = "./upag.sqlite"
	standalonePIDFile    = "./upag.pid"
	standaloneLogFile    = "./upag.log"
)

var packageDefaultsPath = "/etc/default/upag"

type pathDefaults struct {
	ConfigPath string
	DBPath     string
	PIDFile    string
	LogFile    string
}

type pathDefaultTarget struct {
	FlagName string
	Value    *string
	Default  func(pathDefaults) string
}

func standalonePathDefaults() pathDefaults {
	return pathDefaults{
		ConfigPath: standaloneConfigPath,
		DBPath:     standaloneDBPath,
		PIDFile:    standalonePIDFile,
		LogFile:    standaloneLogFile,
	}
}

func applyPathDefaults(fs *flag.FlagSet, targets ...pathDefaultTarget) error {
	needsDefaults := false
	for _, target := range targets {
		if !flagWasSet(fs, target.FlagName) {
			needsDefaults = true
			break
		}
	}
	if !needsDefaults {
		return nil
	}

	defaults, err := loadPathDefaults()
	if err != nil {
		return err
	}
	for _, target := range targets {
		if !flagWasSet(fs, target.FlagName) {
			*target.Value = target.Default(defaults)
		}
	}
	return nil
}

func flagWasSet(fs *flag.FlagSet, name string) bool {
	wasSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			wasSet = true
		}
	})
	return wasSet
}

func loadPathDefaults() (pathDefaults, error) {
	defaults := standalonePathDefaults()
	file, err := os.Open(packageDefaultsPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return defaults, nil
		}
		return defaults, fmt.Errorf("read defaults %q: %w", packageDefaultsPath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for lineNumber := 1; scanner.Scan(); lineNumber++ {
		key, value, ok, err := parseDefaultsLine(scanner.Text())
		if err != nil {
			return defaults, fmt.Errorf("parse defaults %q line %d: %w", packageDefaultsPath, lineNumber, err)
		}
		if !ok {
			continue
		}
		switch key {
		case "UPAG_CONFIG":
			defaults.ConfigPath = value
		case "UPAG_DB":
			defaults.DBPath = value
		case "UPAG_PIDFILE":
			defaults.PIDFile = value
		}
	}
	if err := scanner.Err(); err != nil {
		return defaults, fmt.Errorf("read defaults %q: %w", packageDefaultsPath, err)
	}
	return defaults, nil
}

func parseDefaultsLine(line string) (string, string, bool, error) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false, nil
	}

	key, value, found := strings.Cut(line, "=")
	if !found {
		key = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		if strings.HasPrefix(key, "UPAG_") {
			return "", "", false, fmt.Errorf("expected KEY=value assignment")
		}
		return "", "", false, nil
	}
	key = strings.TrimSpace(key)
	value = strings.TrimSpace(value)
	if key == "" {
		return "", "", false, fmt.Errorf("empty key")
	}
	if strings.HasPrefix(key, "export ") {
		key = strings.TrimSpace(strings.TrimPrefix(key, "export "))
	}
	if !isDefaultsKey(key) {
		return "", "", true, nil
	}

	parsed, err := parseDefaultsValue(value)
	if err != nil {
		return "", "", false, fmt.Errorf("%s: %w", key, err)
	}
	return key, parsed, true, nil
}

func isDefaultsKey(key string) bool {
	switch key {
	case "UPAG_CONFIG", "UPAG_DB", "UPAG_PIDFILE":
		return true
	default:
		return false
	}
}

func parseDefaultsValue(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if value[0] != '\'' && value[0] != '"' {
		return value, nil
	}
	quote := value[0]
	if len(value) < 2 || value[len(value)-1] != quote {
		return "", fmt.Errorf("unterminated quoted value")
	}
	return value[1 : len(value)-1], nil
}
