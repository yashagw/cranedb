package session

import (
	"sync"
)

// Session holds session-level variables for a client connection
type Session struct {
	mu        sync.RWMutex
	variables map[string]interface{}
	// stores *transaction.Transaction, kept as interface{} to avoid circular imports
	activeTx interface{}
}

// NewSession creates a new session with default values
func NewSession() *Session {
	return &Session{
		variables: make(map[string]interface{}),
	}
}

// SetVariable sets a session variable
func (s *Session) SetVariable(name string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.variables[name] = value
}

// GetVariable gets a session variable, returns nil if not set
func (s *Session) GetVariable(name string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.variables[name]
}

// GetBoolVariable gets a boolean session variable, returns false if not set or not a bool
func (s *Session) GetBoolVariable(name string) bool {
	val := s.GetVariable(name)
	if val == nil {
		return false
	}
	if b, ok := val.(bool); ok {
		return b
	}
	return false
}

// GetStringVariable gets a string session variable, returns empty string if not set or not a string
func (s *Session) GetStringVariable(name string) string {
	val := s.GetVariable(name)
	if val == nil {
		return ""
	}
	if s, ok := val.(string); ok {
		return s
	}
	return ""
}

// SetTransaction sets the active transaction for this session
func (s *Session) SetTransaction(tx interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTx = tx
}

// GetTransaction returns the active transaction, or nil if none
func (s *Session) GetTransaction() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeTx
}

// ClearTransaction clears the active transaction
func (s *Session) ClearTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.activeTx = nil
}

// HasActiveTransaction returns true if there is an active transaction
func (s *Session) HasActiveTransaction() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeTx != nil
}
