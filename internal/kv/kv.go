// Package kv is the replicated state machine sitting on top of Raft: a simple
// in-memory key/value map. Every mutation is encoded as a Command, replicated
// through the Raft log, and applied here only once it is committed, so all
// nodes converge on identical state.
package kv

import (
	"encoding/json"
	"sync"
)

// Op is the kind of mutation a Command performs.
type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "delete"
)

// Command is a single state-machine operation. It is JSON-encoded into a Raft
// log entry's opaque Command bytes.
type Command struct {
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Encode marshals a command for storage in the Raft log.
func (c Command) Encode() ([]byte, error) { return json.Marshal(c) }

// Decode unmarshals a command retrieved from the Raft log.
func Decode(b []byte) (Command, error) {
	var c Command
	err := json.Unmarshal(b, &c)
	return c, err
}

// Store is the key/value state machine. It is safe for concurrent use.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

// Apply mutates the store from a committed, decoded command. Only the applier
// (single-threaded, in log order) calls this, but the lock keeps concurrent
// Get calls consistent.
func (s *Store) Apply(cmd Command) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch cmd.Op {
	case OpSet:
		s.data[cmd.Key] = cmd.Value
	case OpDelete:
		delete(s.data, cmd.Key)
	}
}

// Get returns the value for key and whether it is present.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Len returns the number of keys currently stored.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}
