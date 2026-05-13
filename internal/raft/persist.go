package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Persister is the durable-storage abstraction Raft uses to survive crashes.
// currentTerm, votedFor and the log must hit stable storage before a node
// responds to an RPC that depends on them, otherwise the safety guarantees are
// lost across a restart.
type Persister interface {
	Save(state []byte) error
	Load() ([]byte, error)
}

// persistentState is the on-disk representation of a node's durable Raft state.
type persistentState struct {
	CurrentTerm int     `json:"currentTerm"`
	VotedFor    string  `json:"votedFor"`
	Log         []Entry `json:"log"`
}

// FilePersister stores the durable state as a single JSON file. Writes are
// atomic (write to a temp file, then rename) so a crash mid-write cannot
// corrupt the existing state.
type FilePersister struct {
	mu   sync.Mutex
	path string
}

// NewFilePersister returns a persister backed by <dir>/state.json, creating dir
// if necessary.
func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FilePersister{path: filepath.Join(dir, "state.json")}, nil
}

func (p *FilePersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	tmp := p.path + ".tmp"
	if err := os.WriteFile(tmp, state, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p.path)
}

func (p *FilePersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := os.ReadFile(p.path)
	if os.IsNotExist(err) {
		return nil, nil // fresh node, nothing persisted yet
	}
	return data, err
}

// MemPersister keeps the durable state in memory. It is used by the tests: to
// simulate a crash+restart we keep the same MemPersister instance and build a
// new Raft node from it, exactly as a real node would reload from disk.
type MemPersister struct {
	mu    sync.Mutex
	state []byte
}

func NewMemPersister() *MemPersister { return &MemPersister{} }

func (p *MemPersister) Save(state []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = append([]byte(nil), state...)
	return nil
}

func (p *MemPersister) Load() ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.state...), nil
}

// persist writes the durable state. Callers must hold rf.mu.
func (rf *Raft) persist() {
	st := persistentState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	}
	data, err := json.Marshal(st)
	if err != nil {
		panic("raft: failed to encode persistent state: " + err.Error())
	}
	if err := rf.persister.Save(data); err != nil {
		panic("raft: failed to persist state: " + err.Error())
	}
}

// loadPersisted restores durable state on boot. Callers must hold rf.mu. If
// nothing was persisted the node starts fresh with a single sentinel entry.
func (rf *Raft) loadPersisted() {
	data, err := rf.persister.Load()
	if err != nil {
		panic("raft: failed to load persistent state: " + err.Error())
	}
	if len(data) == 0 {
		return
	}
	var st persistentState
	if err := json.Unmarshal(data, &st); err != nil {
		panic("raft: failed to decode persistent state: " + err.Error())
	}
	rf.currentTerm = st.CurrentTerm
	rf.votedFor = st.VotedFor
	if len(st.Log) > 0 {
		rf.log = st.Log
	}
}
