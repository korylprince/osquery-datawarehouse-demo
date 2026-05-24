package session

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/httpapi"
	"github.com/korylprince/osquery-datawarehouse-demo/images/data-warehouse-mcp/internal/util"
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionExpired  = errors.New("session expired")
)

type Config struct {
	Root          string
	RootfsArchive string
	Timeout       time.Duration
	SweepInterval time.Duration
	MaxSessions   int
}

type Manager struct {
	cfg     Config
	logger  *slog.Logger
	metrics *httpapi.Metrics

	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewManager(cfg Config, logger *slog.Logger, metrics *httpapi.Metrics) (*Manager, error) {
	if cfg.Root == "" {
		return nil, fmt.Errorf("sessions root is required")
	}
	if cfg.RootfsArchive == "" {
		return nil, fmt.Errorf("session rootfs archive is required")
	}
	if err := os.MkdirAll(cfg.Root, 0o755); err != nil {
		return nil, fmt.Errorf("create sessions root: %w", err)
	}
	if _, err := os.Stat(cfg.RootfsArchive); err != nil {
		return nil, fmt.Errorf("stat session rootfs archive: %w", err)
	}
	return &Manager{
		cfg:      cfg,
		logger:   logger,
		metrics:  metrics,
		sessions: make(map[string]*Session),
	}, nil
}

func (m *Manager) Ready() error {
	if _, err := os.Stat(m.cfg.RootfsArchive); err != nil {
		return fmt.Errorf("session rootfs archive unavailable: %w", err)
	}
	return nil
}

func (m *Manager) Start(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.SweepInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.SweepExpired(context.Background())
			}
		}
	}()
}

func (m *Manager) Create(ctx context.Context) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.sessions) >= m.cfg.MaxSessions {
		oldest := m.lruSession()
		if oldest == nil {
			return nil, fmt.Errorf("session capacity exceeded")
		}
		evictedMeta := oldest.Snapshot()
		delete(m.sessions, oldest.ID)
		m.metrics.SessionsActive.Dec()
		m.metrics.SessionsExpired.Inc()
		m.logger.Info("evicting oldest session to make room for new session",
			"evicted_session_id", oldest.ID,
			"last_activity_at", evictedMeta.LastActivityAt)
		go m.cleanupSession(context.Background(), oldest)
	}

	id, err := util.NewULID()
	if err != nil {
		return nil, fmt.Errorf("generate session id: %w", err)
	}
	if _, exists := m.sessions[id]; exists {
		return nil, fmt.Errorf("session id collision")
	}

	sessionDir := filepath.Join(m.cfg.Root, id)
	rootfsDir := filepath.Join(sessionDir, ".sandbox-rootfs")
	metaPath := filepath.Join(sessionDir, "meta.json")

	if err := os.MkdirAll(rootfsDir, 0o755); err != nil {
		return nil, fmt.Errorf("create session dir: %w", err)
	}
	if err := extractRootfs(ctx, m.cfg.RootfsArchive, rootfsDir); err != nil {
		_ = os.RemoveAll(sessionDir)
		return nil, err
	}

	now := time.Now().UTC()
	s := &Session{
		ID:       id,
		Dir:      sessionDir,
		RootFS:   rootfsDir,
		MetaPath: metaPath,
		meta: Metadata{
			SessionID:      id,
			CreatedAt:      now,
			LastActivityAt: now,
			ExpiresAt:      now.Add(m.cfg.Timeout),
		},
		processGroups: make(map[int]struct{}),
	}
	if err := m.writeMeta(s); err != nil {
		_ = os.RemoveAll(sessionDir)
		return nil, err
	}

	m.sessions[id] = s
	m.metrics.SessionsActive.Inc()
	m.metrics.SessionsCreated.Inc()
	return s, nil
}

func (m *Manager) GetAndTouch(ctx context.Context, sessionID string) (*Session, error) {
	m.mu.RLock()
	s, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	if !ok {
		return nil, ErrSessionNotFound
	}
	meta := s.Snapshot()
	if time.Now().UTC().After(meta.ExpiresAt) {
		m.expireSession(ctx, s)
		return nil, ErrSessionExpired
	}
	updated := s.updateActivity(time.Now().UTC(), m.cfg.Timeout)
	if err := m.writeMeta(s); err != nil {
		return nil, err
	}
	m.logger.Debug("session touched", "session_id", sessionID, "expires_at", updated.ExpiresAt)
	return s, nil
}

func (m *Manager) RegisterProcessGroup(sessionID string, pgid int) error {
	m.mu.RLock()
	s, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	if !ok {
		return ErrSessionNotFound
	}
	s.addProcessGroup(pgid)
	return nil
}

func (m *Manager) UnregisterProcessGroup(sessionID string, pgid int) {
	m.mu.RLock()
	s, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	if !ok {
		return
	}
	s.removeProcessGroup(pgid)
}

func (m *Manager) KillProcessGroup(pgid int) {
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	for range 10 {
		if err := syscall.Kill(-pgid, 0); err != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}

func (m *Manager) SweepExpired(ctx context.Context) int {
	now := time.Now().UTC()
	var expired []*Session
	m.mu.RLock()
	for _, s := range m.sessions {
		if now.After(s.Snapshot().ExpiresAt) {
			expired = append(expired, s)
		}
	}
	m.mu.RUnlock()

	count := 0
	for _, s := range expired {
		// Cleanup first: kill processes and remove the session directory.
		// If cleanup fails, leave the session in the map so the next sweep can retry.
		if !m.cleanupSession(ctx, s) {
			continue
		}
		// Remove from the in-memory map only after successful filesystem cleanup.
		m.mu.Lock()
		if _, ok := m.sessions[s.ID]; ok {
			delete(m.sessions, s.ID)
			m.metrics.SessionsActive.Dec()
			m.metrics.SessionsExpired.Inc()
		}
		m.mu.Unlock()
		count++
	}
	return count
}

func (m *Manager) expireSession(ctx context.Context, s *Session) {
	// Best-effort cleanup; always remove from map for user-facing expiry paths.
	m.cleanupSession(ctx, s)
	m.mu.Lock()
	if _, ok := m.sessions[s.ID]; ok {
		delete(m.sessions, s.ID)
		m.metrics.SessionsActive.Dec()
		m.metrics.SessionsExpired.Inc()
	}
	m.mu.Unlock()
}

// cleanupSession kills all tracked process groups and removes the session directory.
// Returns true if the session directory was successfully removed; false on failure
// (so the sweeper can retry).
func (m *Manager) cleanupSession(_ context.Context, s *Session) bool {
	for _, pgid := range s.processGroupList() {
		m.KillProcessGroup(pgid)
	}
	dirRemoved := true
	if err := os.RemoveAll(s.Dir); err != nil {
		m.logger.Warn("failed to remove session directory", "session_id", s.ID, "error", err.Error())
		dirRemoved = false
	}
	return dirRemoved
}

// lruSession returns the session with the oldest LastActivityAt (least-recently-used).
// Must be called with m.mu held.
func (m *Manager) lruSession() *Session {
	var oldest *Session
	for _, s := range m.sessions {
		if oldest == nil || s.Snapshot().LastActivityAt.Before(oldest.Snapshot().LastActivityAt) {
			oldest = s
		}
	}
	return oldest
}


func (m *Manager) writeMeta(s *Session) error {
	meta := s.Snapshot()
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal session metadata: %w", err)
	}
	if err := os.WriteFile(s.MetaPath, data, 0o644); err != nil {
		return fmt.Errorf("write session metadata: %w", err)
	}
	return nil
}
