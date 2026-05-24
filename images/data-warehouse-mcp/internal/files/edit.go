package files

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Edit is a single old→new string replacement for ApplyEdits.
type Edit struct {
	OldStr string `json:"old_str"`
	NewStr string `json:"new_str"`
}

func WriteTextFile(workspaceRoot, inputPath, content string, maxBytes int64) (string, int64, error) {
	normalized, err := NormalizeWorkspacePath(inputPath)
	if err != nil {
		return "", 0, err
	}
	if int64(len(content)) > maxBytes {
		return "", 0, fmt.Errorf("result exceeds maximum allowed size")
	}
	hostPath, err := ResolvePath(workspaceRoot, normalized, true)
	if err != nil {
		return "", 0, err
	}
	if err := os.MkdirAll(filepath.Dir(hostPath), 0o755); err != nil {
		return "", 0, fmt.Errorf("create parent directories: %w", err)
	}
	if err := os.WriteFile(hostPath, []byte(content), 0o644); err != nil {
		return "", 0, fmt.Errorf("write file: %w", err)
	}
	return normalized, int64(len(content)), nil
}

// ApplyEdits applies a sequence of targeted string replacements to an existing file.
// Each Edit's OldStr must match exactly one occurrence; if any match zero or more than
// one occurrences, the entire operation is rejected and the file is left unchanged.
func ApplyEdits(workspaceRoot, inputPath string, edits []Edit, maxBytes int64) (string, int64, error) {
	if len(edits) == 0 {
		return "", 0, fmt.Errorf("edits must contain at least one entry")
	}
	normalized, err := NormalizeWorkspacePath(inputPath)
	if err != nil {
		return "", 0, err
	}
	hostPath, err := ResolvePath(workspaceRoot, normalized, false)
	if err != nil {
		return "", 0, err
	}
	data, err := os.ReadFile(hostPath)
	if err != nil {
		return "", 0, fmt.Errorf("read file: %w", err)
	}
	content := string(data)
	for i, edit := range edits {
		if edit.OldStr == "" {
			return "", 0, fmt.Errorf("edit %d: old_str must not be empty", i+1)
		}
		count := strings.Count(content, edit.OldStr)
		if count == 0 {
			return "", 0, fmt.Errorf("edit %d: old_str not found in file", i+1)
		}
		if count > 1 {
			return "", 0, fmt.Errorf("edit %d: old_str matches %d occurrences, must match exactly one", i+1, count)
		}
		content = strings.Replace(content, edit.OldStr, edit.NewStr, 1)
	}
	if int64(len(content)) > maxBytes {
		return "", 0, fmt.Errorf("result exceeds maximum allowed size")
	}
	if err := os.WriteFile(hostPath, []byte(content), 0o644); err != nil {
		return "", 0, fmt.Errorf("write file: %w", err)
	}
	return normalized, int64(len(content)), nil
}
