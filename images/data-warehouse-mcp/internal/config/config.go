package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	ListenAddr           string
	TrinoHost            string
	TrinoPort            int
	TrinoScheme          string
	TrinoCatalog         string
	TrinoSchema          string
	TrinoSource          string
	SessionTimeout       time.Duration
	SessionSweepInterval time.Duration
	MaxSessions          int
	MaxInlineRows        int
	MaxQueryFileBytes    int64
	MaxBashOutputBytes   int64
	DefaultBashTimeout   time.Duration
	MaxBashTimeout       time.Duration
	DefaultQueryTimeout  time.Duration
	MaxQueryTimeout      time.Duration
	MaxEditBytes         int64
	MaxImageBytes        int64
	SessionsRoot         string
	SessionRootfsArchive string
	ImageBaseURL         string
	ImagesRoot           string
}

func Parse() (Config, error) {
	cfg := Config{
		ListenAddr:           getEnv("LISTEN_ADDR", ":8080"),
		TrinoHost:            strings.TrimSpace(os.Getenv("TRINO_HOST")),
		TrinoPort:            getEnvInt("TRINO_PORT", 8080),
		TrinoScheme:          getEnv("TRINO_SCHEME", "http"),
		TrinoCatalog:         strings.TrimSpace(os.Getenv("TRINO_CATALOG")),
		TrinoSchema:          strings.TrimSpace(os.Getenv("TRINO_SCHEMA")),
		TrinoSource:          getEnv("TRINO_SOURCE", "data-warehouse-mcp"),
		SessionTimeout:       getEnvDuration("SESSION_TIMEOUT", time.Hour),
		SessionSweepInterval: getEnvDuration("SESSION_SWEEP_INTERVAL", 5*time.Minute),
		MaxSessions:          getEnvInt("MAX_SESSIONS", 10),
		MaxInlineRows:        getEnvInt("MAX_INLINE_ROWS", 100),
		MaxQueryFileBytes:    getEnvInt64("MAX_QUERY_FILE_BYTES", 104857600),
		MaxBashOutputBytes:   getEnvInt64("MAX_BASH_OUTPUT_BYTES", 1048576),
		DefaultBashTimeout:   getEnvDuration("DEFAULT_BASH_TIMEOUT", 300*time.Second),
		MaxBashTimeout:       getEnvDuration("MAX_BASH_TIMEOUT", 900*time.Second),
		DefaultQueryTimeout:  getEnvDuration("DEFAULT_QUERY_TIMEOUT", 120*time.Second),
		MaxQueryTimeout:      getEnvDuration("MAX_QUERY_TIMEOUT", 900*time.Second),
		MaxEditBytes:         getEnvInt64("MAX_EDIT_BYTES", 1048576),
		MaxImageBytes:        getEnvInt64("MAX_IMAGE_BYTES", 10485760),
		SessionsRoot:         getEnv("SESSIONS_ROOT", "/sessions"),
		SessionRootfsArchive: getEnv("SESSION_ROOTFS_ARCHIVE", "/opt/session-rootfs.tar"),
		ImageBaseURL:         strings.TrimRight(strings.TrimSpace(os.Getenv("IMAGE_BASE_URL")), "/"),
		ImagesRoot:           getEnv("IMAGES_ROOT", "/images"),
	}

	if cfg.TrinoHost == "" {
		return Config{}, fmt.Errorf("TRINO_HOST is required")
	}
	if cfg.TrinoCatalog == "" {
		return Config{}, fmt.Errorf("TRINO_CATALOG is required")
	}
	if cfg.TrinoSchema == "" {
		return Config{}, fmt.Errorf("TRINO_SCHEMA is required")
	}
	if cfg.ImageBaseURL == "" {
		return Config{}, fmt.Errorf("IMAGE_BASE_URL is required")
	}
	if cfg.TrinoScheme != "http" && cfg.TrinoScheme != "https" {
		return Config{}, fmt.Errorf("TRINO_SCHEME must be http or https")
	}
	if cfg.TrinoPort <= 0 {
		return Config{}, fmt.Errorf("TRINO_PORT must be greater than zero")
	}
	if cfg.MaxSessions <= 0 {
		return Config{}, fmt.Errorf("MAX_SESSIONS must be greater than zero")
	}
	if cfg.MaxInlineRows <= 0 {
		return Config{}, fmt.Errorf("MAX_INLINE_ROWS must be greater than zero")
	}
	for name, value := range map[string]int64{
		"MAX_QUERY_FILE_BYTES":  cfg.MaxQueryFileBytes,
		"MAX_BASH_OUTPUT_BYTES": cfg.MaxBashOutputBytes,
		"MAX_EDIT_BYTES":        cfg.MaxEditBytes,
		"MAX_IMAGE_BYTES":       cfg.MaxImageBytes,
	} {
		if value <= 0 {
			return Config{}, fmt.Errorf("%s must be greater than zero", name)
		}
	}
	for name, value := range map[string]time.Duration{
		"SESSION_TIMEOUT":        cfg.SessionTimeout,
		"SESSION_SWEEP_INTERVAL": cfg.SessionSweepInterval,
		"DEFAULT_BASH_TIMEOUT":   cfg.DefaultBashTimeout,
		"MAX_BASH_TIMEOUT":       cfg.MaxBashTimeout,
		"DEFAULT_QUERY_TIMEOUT":  cfg.DefaultQueryTimeout,
		"MAX_QUERY_TIMEOUT":      cfg.MaxQueryTimeout,
	} {
		if value <= 0 {
			return Config{}, fmt.Errorf("%s must be greater than zero", name)
		}
	}
	if cfg.DefaultBashTimeout > cfg.MaxBashTimeout {
		return Config{}, fmt.Errorf("DEFAULT_BASH_TIMEOUT cannot exceed MAX_BASH_TIMEOUT")
	}
	if cfg.DefaultQueryTimeout > cfg.MaxQueryTimeout {
		return Config{}, fmt.Errorf("DEFAULT_QUERY_TIMEOUT cannot exceed MAX_QUERY_TIMEOUT")
	}
	cfg.SessionsRoot = filepath.Clean(cfg.SessionsRoot)
	cfg.SessionRootfsArchive = filepath.Clean(cfg.SessionRootfsArchive)
	cfg.ImagesRoot = filepath.Clean(cfg.ImagesRoot)
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		parsed, err := strconv.Atoi(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func getEnvInt64(key string, fallback int64) int64 {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		parsed, err := time.ParseDuration(value)
		if err == nil {
			return parsed
		}
	}
	return fallback
}
