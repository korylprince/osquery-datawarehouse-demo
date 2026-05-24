package files

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeWorkspacePath(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{input: "plot.py", want: "/plot.py"},
		{input: "/charts/out.png", want: "/charts/out.png"},
		{input: "./results/../plot.py", want: "/plot.py"},
		{input: "../../etc/passwd", wantErr: true},
	}
	for _, tc := range tests {
		got, err := NormalizeWorkspacePath(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("expected error for %q", tc.input)
			}
			continue
		}
		if err != nil {
			t.Fatalf("normalize %q: %v", tc.input, err)
		}
		if got != tc.want {
			t.Fatalf("normalize %q = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestResolvePathRejectsEscapingSymlink(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(filepath.Join(root, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "dir", "escape")); err != nil {
		t.Fatal(err)
	}
	_, err := ResolvePath(root, "/dir/escape/file.txt", true)
	if err == nil {
		t.Fatal("expected symlink escape to fail")
	}
}
