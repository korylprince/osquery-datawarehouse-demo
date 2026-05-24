package session

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/httpapi"
)

func TestManagerCreateAndSweep(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "rootfs.tar")
	writeTestArchive(t, archive)

	metrics := httpapi.NewMetrics()
	mgr, err := NewManager(Config{
		Root:          filepath.Join(t.TempDir(), "sessions"),
		RootfsArchive: archive,
		Timeout:       50 * time.Millisecond,
		SweepInterval: 10 * time.Millisecond,
		MaxSessions:   2,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	sess, err := mgr.Create(context.Background())
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := os.Stat(sess.RootFS); err != nil {
		t.Fatalf("rootfs missing: %v", err)
	}
	time.Sleep(70 * time.Millisecond)
	if count := mgr.SweepExpired(context.Background()); count != 1 {
		t.Fatalf("expected one expired session, got %d", count)
	}
}

func TestManagerEvictsOldestOnFull(t *testing.T) {
	archive := filepath.Join(t.TempDir(), "rootfs.tar")
	writeTestArchive(t, archive)

	metrics := httpapi.NewMetrics()
	mgr, err := NewManager(Config{
		Root:          filepath.Join(t.TempDir(), "sessions"),
		RootfsArchive: archive,
		Timeout:       10 * time.Minute,
		SweepInterval: time.Hour,
		MaxSessions:   1,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), metrics)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	first, err := mgr.Create(context.Background())
	if err != nil {
		t.Fatalf("create first session: %v", err)
	}
	firstID := first.ID

	// Creating a second session should evict the first.
	second, err := mgr.Create(context.Background())
	if err != nil {
		t.Fatalf("create second session (eviction expected): %v", err)
	}

	if _, err := mgr.GetAndTouch(context.Background(), firstID); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("evicted session should not be found, got: %v", err)
	}
	if _, err := mgr.GetAndTouch(context.Background(), second.ID); err != nil {
		t.Fatalf("new session should be accessible: %v", err)
	}
}


func writeTestArchive(t *testing.T, path string) {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range []string{"tmp/", "usr/", "bin/", "etc/"} {
		hdr := &tar.Header{Name: name, Mode: 0o755, Typeflag: tar.TypeDir}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}
