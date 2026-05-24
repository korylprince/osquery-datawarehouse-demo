package httpapi

import (
	"net/http"
	"os"
	"path/filepath"
	"regexp"
)

// imageFilenamePattern restricts served filenames to ULID (26 Crockford base32 chars) plus
// a known extension. This prevents path traversal and limits exposure to published images only.
var imageFilenamePattern = regexp.MustCompile(`^[0-9A-Z]{26}\.(png|jpg|gif|webp)$`)

// ImageHandler serves published images from imagesRoot at GET /images/{filename}.
func ImageHandler(imagesRoot string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		filename := r.PathValue("filename")
		if !imageFilenamePattern.MatchString(filename) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		filePath := filepath.Join(imagesRoot, filename)
		data, err := os.ReadFile(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				http.Error(w, "not found", http.StatusNotFound)
			} else {
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
			return
		}
		mimeType := http.DetectContentType(data)
		w.Header().Set("Content-Type", mimeType)
		w.Header().Set("Cache-Control", "public, max-age=3600")
		_, _ = w.Write(data)
	}
}
