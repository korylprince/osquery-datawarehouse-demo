package session

import (
	"sync"
	"time"
)

type Metadata struct {
	SessionID      string    `json:"session_id"`
	CreatedAt      time.Time `json:"created_at"`
	LastActivityAt time.Time `json:"last_activity_at"`
	ExpiresAt      time.Time `json:"expires_at"`
}

type Session struct {
	ID       string
	Dir      string
	RootFS   string
	MetaPath string

	mu            sync.Mutex
	meta          Metadata
	processGroups map[int]struct{}
}

func (s *Session) Snapshot() Metadata {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.meta
}

func (s *Session) updateActivity(now time.Time, timeout time.Duration) Metadata {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.meta.LastActivityAt = now.UTC()
	s.meta.ExpiresAt = now.UTC().Add(timeout)
	return s.meta
}

func (s *Session) addProcessGroup(pgid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.processGroups[pgid] = struct{}{}
}

func (s *Session) removeProcessGroup(pgid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.processGroups, pgid)
}

func (s *Session) processGroupList() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	groups := make([]int, 0, len(s.processGroups))
	for pgid := range s.processGroups {
		groups = append(groups, pgid)
	}
	return groups
}

