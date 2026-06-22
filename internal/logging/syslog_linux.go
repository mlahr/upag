//go:build linux

package logging

import (
	"io"
	"log/syslog"
	"strings"
)

type SyslogLogger struct {
	writer *syslog.Writer
}

type syslogPriorityWriter struct {
	writer   *syslog.Writer
	priority syslog.Priority
}

func OpenSyslog(tag string) (*SyslogLogger, error) {
	writer, err := syslog.New(syslog.LOG_DAEMON|syslog.LOG_INFO, tag)
	if err != nil {
		return nil, err
	}
	return &SyslogLogger{writer: writer}, nil
}

func (l *SyslogLogger) InfoWriter() io.Writer {
	return syslogPriorityWriter{writer: l.writer, priority: syslog.LOG_INFO}
}

func (l *SyslogLogger) ErrorWriter() io.Writer {
	return syslogPriorityWriter{writer: l.writer, priority: syslog.LOG_ERR}
}

func (l *SyslogLogger) Close() error {
	return l.writer.Close()
}

func (w syslogPriorityWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\r\n")
	var err error
	switch w.priority {
	case syslog.LOG_ERR:
		err = w.writer.Err(msg)
	default:
		err = w.writer.Info(msg)
	}
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
