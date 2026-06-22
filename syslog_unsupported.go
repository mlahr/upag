//go:build !linux

package main

import (
	"fmt"
	"io"
)

type syslogLogger struct{}

func openSyslog(tag string) (*syslogLogger, error) {
	return nil, fmt.Errorf("syslog logging is supported only on Linux")
}

func (l *syslogLogger) InfoWriter() io.Writer {
	return io.Discard
}

func (l *syslogLogger) ErrorWriter() io.Writer {
	return io.Discard
}

func (l *syslogLogger) Close() error {
	return nil
}
