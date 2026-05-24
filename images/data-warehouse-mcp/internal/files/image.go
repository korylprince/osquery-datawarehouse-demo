package files

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/util"
)

var mimeExtensions = map[string]string{
	"image/png":  "png",
	"image/jpeg": "jpg",
	"image/gif":  "gif",
	"image/webp": "webp",
}

// PublishImageFile copies an image from the session workspace into imagesRoot,
// then validates the MIME type of the copy. Validating after copying prevents a
// TOCTOU race where the agent could replace the workspace file between validation
// and the copy. The caller is responsible for tracking the returned host path for cleanup.
func PublishImageFile(workspaceRoot, inputPath, imagesRoot string, maxBytes int64) (filename, mimeType, hostPath string, err error) {
	normalized, err := NormalizeWorkspacePath(inputPath)
	if err != nil {
		return "", "", "", err
	}
	srcPath, err := ResolvePath(workspaceRoot, normalized, false)
	if err != nil {
		return "", "", "", err
	}

	id, err := util.NewULID()
	if err != nil {
		return "", "", "", fmt.Errorf("generate image id: %w", err)
	}

	// Copy to a temp path in imagesRoot (outside the workspace) first.
	// MIME validation happens on this copy, not the original workspace file.
	tmpPath := filepath.Join(imagesRoot, id+".tmp")
	if err := copyFileWithLimit(srcPath, tmpPath, maxBytes); err != nil {
		return "", "", "", err
	}

	mime, ext, err := detectImageMIME(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", "", "", err
	}

	// Rename to the final filename now that the type is confirmed.
	filename = id + "." + ext
	finalPath := filepath.Join(imagesRoot, filename)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return "", "", "", fmt.Errorf("publish image: %w", err)
	}

	return filename, mime, finalPath, nil
}

// copyFileWithLimit copies src to dst (created exclusively), capping the copy at
// maxBytes. Returns an error if the source exceeds maxBytes.
func copyFileWithLimit(src, dst string, maxBytes int64) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open source image: %w", err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return fmt.Errorf("create temp image: %w", err)
	}
	defer out.Close()

	n, err := io.Copy(out, io.LimitReader(in, maxBytes+1))
	if err != nil {
		return fmt.Errorf("copy image: %w", err)
	}
	if n > maxBytes {
		return fmt.Errorf("image exceeds maximum allowed size")
	}
	return nil
}

// detectImageMIME reads the first 512 bytes of path (sufficient for
// http.DetectContentType) and returns the MIME type and file extension.
func detectImageMIME(path string) (mimeType, ext string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", fmt.Errorf("open image for detection: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 512)
	n, err := io.ReadFull(f, buf)
	if errors.Is(err, io.EOF) {
		return "", "", fmt.Errorf("image file is empty")
	}
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
		return "", "", fmt.Errorf("read image header: %w", err)
	}

	mime := http.DetectContentType(buf[:n])
	e, ok := mimeExtensions[mime]
	if !ok {
		return "", "", fmt.Errorf("unsupported image type: %s", mime)
	}
	return mime, e, nil
}
