//go:build !linux

package logging

import (
	"fmt"
	"io"
)

type SyslogLogger struct{}

func OpenSyslog(tag string) (*SyslogLogger, error) {
	return nil, fmt.Errorf("syslog logging is supported only on Linux")
}

func (l *SyslogLogger) InfoWriter() io.Writer {
	return io.Discard
}

func (l *SyslogLogger) ErrorWriter() io.Writer {
	return io.Discard
}

func (l *SyslogLogger) Close() error {
	return nil
}
