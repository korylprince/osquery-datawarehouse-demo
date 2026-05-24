package files

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var ErrPathEscape = errors.New("path must stay inside the session workspace")

func NormalizeWorkspacePath(input string) (string, error) {
	if strings.ContainsRune(input, '\x00') {
		return "", fmt.Errorf("path contains NUL byte")
	}
	candidate := input
	if candidate == "" {
		return "", fmt.Errorf("path is required")
	}
	if !strings.HasPrefix(candidate, "/") {
		candidate = "/" + candidate
	}
	if escapesRoot(candidate) {
		return "", ErrPathEscape
	}
	cleaned := path.Clean(candidate)
	if cleaned == "." {
		cleaned = "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		return "", ErrPathEscape
	}
	return cleaned, nil
}

func ResolvePath(workspaceRoot, normalizedPath string, allowMissing bool) (string, error) {
	base, err := filepath.EvalSymlinks(workspaceRoot)
	if err != nil {
		return "", fmt.Errorf("resolve workspace root: %w", err)
	}
	target := filepath.Join(base, filepath.FromSlash(strings.TrimPrefix(normalizedPath, "/")))
	current := target
	for {
		_, err := os.Lstat(current)
		if err == nil {
			resolved, err := filepath.EvalSymlinks(current)
			if err != nil {
				return "", fmt.Errorf("resolve path: %w", err)
			}
			if !withinBase(base, resolved) {
				return "", ErrPathEscape
			}
			return target, nil
		}
		if !os.IsNotExist(err) {
			return "", err
		}
		if current == target && !allowMissing {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", ErrPathEscape
		}
		current = parent
	}
}

func escapesRoot(candidate string) bool {
	depth := 0
	for _, segment := range strings.Split(candidate, "/") {
		switch segment {
		case "", ".":
			continue
		case "..":
			if depth == 0 {
				return true
			}
			depth--
		default:
			depth++
		}
	}
	return false
}

func withinBase(base, target string) bool {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return false
	}
	return rel == "." || (!strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != "..")
}
