package sandbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/httpapi"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/session"
)

type Config struct {
	DefaultTimeout time.Duration
	MaxTimeout     time.Duration
	MaxOutputBytes int64
}

// sandboxPATH is the PATH searched when resolving a bare command name inside the chroot.
var sandboxPATH = []string{
	"/usr/local/sbin", "/usr/local/bin",
	"/usr/sbin", "/usr/bin",
	"/sbin", "/bin",
}

// sandboxEnvBase are the environment variables set for every chrooted command.
var sandboxEnvBase = []string{
	"HOME=/",
	"PWD=/",
	"TMPDIR=/tmp",
	"MPLCONFIGDIR=/tmp/matplotlib",
	"XDG_CACHE_HOME=/tmp/.cache",
	"PYTHONUNBUFFERED=1",
	"PYTHONNOUSERSITE=1",
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}

// lookupInChroot resolves argv[0] to the path that execve should receive after the
// chroot is applied.  If cmd contains a slash it is returned as-is (the caller is
// responsible for using a correct chroot-relative or relative path).  Otherwise the
// sandbox PATH directories are searched inside rootfs and the first match is returned
// as a chroot-absolute path (e.g. "/usr/bin/python3").
func lookupInChroot(rootfs, cmd string) (string, error) {
	if strings.ContainsRune(cmd, '/') {
		return cmd, nil
	}
	for _, dir := range sandboxPATH {
		hostPath := filepath.Join(rootfs, dir, cmd)
		if info, err := os.Stat(hostPath); err == nil && !info.IsDir() {
			return filepath.Join(dir, cmd), nil
		}
	}
	return "", fmt.Errorf("command not found: %s", cmd)
}

type Runner struct {
	cfg     Config
	logger  *slog.Logger
	metrics *httpapi.Metrics
}

type Result struct {
	ExitCode int    `json:"exit_code"`
	Stdout   string `json:"stdout"`
	Stderr   string `json:"stderr"`
	TimedOut bool   `json:"timed_out"`
}

func NewRunner(cfg Config, logger *slog.Logger, metrics *httpapi.Metrics) *Runner {
	return &Runner{cfg: cfg, logger: logger, metrics: metrics}
}

func (r *Runner) Run(ctx context.Context, registry processRegistry, sess *session.Session, argv []string, timeout time.Duration) (Result, error) {
	if len(argv) == 0 {
		return Result{}, fmt.Errorf("argv must contain at least one element")
	}
	if timeout <= 0 {
		timeout = r.cfg.DefaultTimeout
	}
	if timeout > r.cfg.MaxTimeout {
		return Result{}, fmt.Errorf("timeout exceeds maximum allowed value")
	}

	start := time.Now()
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	stdout := newLimitedBuffer(r.cfg.MaxOutputBytes)
	stderr := newLimitedBuffer(r.cfg.MaxOutputBytes)

	// Resolve the command inside the chroot rootfs so execve receives a path
	// that is valid after the chroot is applied.
	binPath, err := lookupInChroot(sess.RootFS, argv[0])
	if err != nil {
		r.metrics.ObserveBash("error", time.Since(start))
		return Result{}, err
	}

	childEnv := make([]string, len(sandboxEnvBase), len(sandboxEnvBase)+1)
	copy(childEnv, sandboxEnvBase)
	childEnv = append(childEnv,
		"DW_SESSION_ID="+sess.ID,
	)

	cmd := &exec.Cmd{
		Path:   binPath,
		Args:   argv,
		Env:    childEnv,
		Dir:    "/",
		Stdout: stdout,
		Stderr: stderr,
		SysProcAttr: &syscall.SysProcAttr{
			Chroot:  sess.RootFS,
			Setpgid: true,
		},
	}

	if err := cmd.Start(); err != nil {
		r.metrics.ObserveBash("error", time.Since(start))
		return Result{}, fmt.Errorf("start command: %w", err)
	}
	pgid := cmd.Process.Pid
	if err := registry.RegisterProcessGroup(sess.ID, pgid); err != nil {
		_ = cmd.Process.Kill()
		r.metrics.ObserveBash("error", time.Since(start))
		return Result{}, err
	}
	defer registry.UnregisterProcessGroup(sess.ID, pgid)

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- cmd.Wait()
	}()

	result := Result{ExitCode: -1}
	var waitErr error
	select {
	case waitErr = <-waitCh:
	case <-runCtx.Done():
		result.TimedOut = errors.Is(runCtx.Err(), context.DeadlineExceeded)
		registry.KillProcessGroup(pgid)
		waitErr = <-waitCh
	}

	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	status := "success"
	if waitErr != nil {
		status = "error"
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			result.ExitCode = exitErr.ExitCode()
			if result.TimedOut {
				status = "timeout"
			}
		} else {
			r.metrics.ObserveBash(status, time.Since(start))
			return result, fmt.Errorf("wait for command: %w", waitErr)
		}
	} else {
		result.ExitCode = 0
	}
	if result.TimedOut && result.ExitCode < 0 {
		result.ExitCode = 124
	}
	r.metrics.ObserveBash(status, time.Since(start))
	r.logger.Info("workspace command completed",
		"session_id", sess.ID,
		"argv", argv,
		"duration_ms", time.Since(start).Milliseconds(),
		"status", status,
		"exit_code", result.ExitCode,
		"timed_out", result.TimedOut,
		"stdout_bytes", strconv.Itoa(len(result.Stdout)),
		"stderr_bytes", strconv.Itoa(len(result.Stderr)),
	)
	return result, nil
}

type processRegistry interface {
	RegisterProcessGroup(sessionID string, pgid int) error
	UnregisterProcessGroup(sessionID string, pgid int)
	KillProcessGroup(pgid int)
}
