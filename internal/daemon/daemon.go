package daemon

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var ErrNotRunning = errors.New("daemon is not running")

type Options struct {
	ConfigPath string
	PIDFile    string
	LogFile    string
	Syslog     bool
}

type Status struct {
	PID       int
	PIDFile   string
	Running   bool
	StaleFile bool
}

func Start(opts Options) (int, error) {
	if err := validateStartOptions(opts); err != nil {
		return 0, err
	}

	status, err := Inspect(opts.PIDFile)
	if err != nil {
		return 0, err
	}
	if status.Running {
		return 0, fmt.Errorf("daemon is already running with PID %d", status.PID)
	}
	if status.StaleFile {
		if err := os.Remove(opts.PIDFile); err != nil {
			return 0, fmt.Errorf("remove stale pid file %q: %w", opts.PIDFile, err)
		}
	}

	var logFile *os.File
	if opts.Syslog {
		var err error
		logFile, err = os.OpenFile(os.DevNull, os.O_WRONLY, 0)
		if err != nil {
			return 0, fmt.Errorf("open %s: %w", os.DevNull, err)
		}
	} else {
		var err error
		logFile, err = os.OpenFile(opts.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return 0, fmt.Errorf("open log file %q: %w", opts.LogFile, err)
		}
	}
	defer logFile.Close()

	exe, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("resolve executable path: %w", err)
	}
	cmdArgs := []string{"run", "--config", opts.ConfigPath}
	if opts.Syslog {
		cmdArgs = append(cmdArgs, "--syslog")
	}
	cmd := exec.Command(exe, cmdArgs...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.Stdin = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start daemon: %w", err)
	}
	pid := cmd.Process.Pid
	if err := cmd.Process.Release(); err != nil {
		return 0, fmt.Errorf("release daemon process: %w", err)
	}
	if err := os.WriteFile(opts.PIDFile, []byte(fmt.Sprintf("%d\n", pid)), 0644); err != nil {
		_ = syscall.Kill(pid, syscall.SIGTERM)
		return 0, fmt.Errorf("write pid file %q: %w", opts.PIDFile, err)
	}
	return pid, nil
}

func validateStartOptions(opts Options) error {
	if opts.PIDFile == "" {
		return errors.New("pid file path is required")
	}
	if opts.LogFile == "" && !opts.Syslog {
		return errors.New("log file path is required")
	}
	return nil
}

func Stop(pidFile string, timeout time.Duration) error {
	status, err := Inspect(pidFile)
	if err != nil {
		return err
	}
	if !status.Running {
		if status.StaleFile {
			_ = os.Remove(pidFile)
		}
		return ErrNotRunning
	}
	if err := syscall.Kill(status.PID, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			_ = os.Remove(pidFile)
			return ErrNotRunning
		}
		return fmt.Errorf("terminate daemon PID %d: %w", status.PID, err)
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processExists(status.PID) {
			if err := os.Remove(pidFile); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("remove pid file %q: %w", pidFile, err)
			}
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("daemon PID %d did not exit within %s", status.PID, timeout)
}

func Reload(pidFile string) (int, error) {
	status, err := Inspect(pidFile)
	if err != nil {
		return 0, err
	}
	if !status.Running {
		if status.StaleFile {
			return 0, fmt.Errorf("daemon is not running; pid file %q is stale for PID %d", pidFile, status.PID)
		}
		return 0, ErrNotRunning
	}
	if err := syscall.Kill(status.PID, syscall.SIGHUP); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return 0, ErrNotRunning
		}
		return 0, fmt.Errorf("reload daemon PID %d: %w", status.PID, err)
	}
	return status.PID, nil
}

func Inspect(pidFile string) (Status, error) {
	status := Status{PIDFile: pidFile}
	pid, err := ReadPID(pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return status, nil
		}
		return status, err
	}
	status.PID = pid
	status.Running = processExists(pid)
	status.StaleFile = !status.Running
	return status, nil
}

func ReadPID(pidFile string) (int, error) {
	data, err := os.ReadFile(pidFile)
	if err != nil {
		return 0, err
	}
	raw := strings.TrimSpace(string(data))
	if raw == "" {
		return 0, fmt.Errorf("pid file %q is empty", pidFile)
	}
	pid, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("pid file %q does not contain a decimal process ID: %w", pidFile, err)
	}
	if pid <= 0 {
		return 0, fmt.Errorf("pid file %q contains non-positive process ID %d", pidFile, pid)
	}
	return pid, nil
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, syscall.Signal(0))
	return err == nil || errors.Is(err, syscall.EPERM)
}
