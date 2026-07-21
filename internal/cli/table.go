package cli

import (
	"bytes"
	"io"
	"os"
	"regexp"
	"strings"
	"text/tabwriter"

	"golang.org/x/term"
)

const (
	ansiReset    = "\x1b[0m"
	ansiBoldCyan = "\x1b[1;36m"
	ansiGreen    = "\x1b[32m"
	ansiYellow   = "\x1b[33m"
	ansiRed      = "\x1b[31m"
	ansiMagenta  = "\x1b[35m"
)

var semanticTableValue = regexp.MustCompile(`\b(?:UP|OK|ACTIVE|yes|DOWN|FAIL|FAILURE|OBSERVER_DOWN|CANCELLED|SUPPRESSED|UPCOMING|ENDED|no)\b`)

// printTable renders before adding ANSI sequences so color never affects column
// width calculation. Color is intentionally limited to interactive terminals.
func printTable(w io.Writer, render func(*tabwriter.Writer) error) error {
	var buffer bytes.Buffer
	tw := tabwriter.NewWriter(&buffer, 0, 0, 2, ' ', 0)
	if err := render(tw); err != nil {
		return err
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	output := buffer.String()
	if tableColorEnabled(w) {
		output = colorTable(output)
	}
	_, err := io.WriteString(w, output)
	return err
}

func tableColorEnabled(w io.Writer) bool {
	if _, disabled := os.LookupEnv("NO_COLOR"); disabled || os.Getenv("TERM") == "dumb" {
		return false
	}
	fdWriter, ok := w.(interface{ Fd() uintptr })
	return ok && term.IsTerminal(int(fdWriter.Fd()))
}

func colorTable(table string) string {
	lines := strings.SplitAfter(table, "\n")
	for index, line := range lines {
		if line == "" {
			continue
		}
		ending := ""
		content := line
		if strings.HasSuffix(content, "\n") {
			content = strings.TrimSuffix(content, "\n")
			ending = "\n"
		}
		if index == 0 {
			lines[index] = ansiBoldCyan + content + ansiReset + ending
			continue
		}
		lines[index] = semanticTableValue.ReplaceAllStringFunc(content, func(value string) string {
			color := ansiYellow
			switch value {
			case "UP", "OK", "ACTIVE", "yes":
				color = ansiGreen
			case "DOWN", "FAIL", "FAILURE", "OBSERVER_DOWN", "CANCELLED", "SUPPRESSED":
				color = ansiRed
			case "UPCOMING":
				color = ansiMagenta
			}
			return color + value + ansiReset
		}) + ending
	}
	return strings.Join(lines, "")
}
