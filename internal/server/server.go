// Package server wires a Raft node and the KV state machine behind an HTTP API.
// A single HTTP server per node serves both the internal Raft RPCs
// (/raft/...) and the client REST API (/kv/..., /status).
package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/CompileWithG/raftkv/internal/kv"
	"github.com/CompileWithG/raftkv/internal/raft"
	"github.com/CompileWithG/raftkv/internal/transport"
)

// Config describes one node.
type Config struct {
	ID      string            // this node's ID
	Addr    string            // listen address, e.g. "127.0.0.1:9001"
	PeerURL map[string]string // peerID -> base URL, for RPCs and client redirects (excludes self)
	DataDir string            // directory for persisted Raft state
}

// Server is a running node.
type Server struct {
	cfg   Config
	rf    *raft.Raft
	store *kv.Store
	http  *http.Server

	mu           sync.Mutex
	appliedIndex int        // highest Raft index applied to the store
	appliedCond  *sync.Cond // signalled when appliedIndex advances
}

// New builds a node: persister, transport, Raft core, KV store, and HTTP mux.
func New(cfg Config) (*Server, error) {
	persister, err := raft.NewFilePersister(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	s := &Server{
		cfg:   cfg,
		store: kv.NewStore(),
	}
	s.appliedCond = sync.NewCond(&s.mu)

	peers := make([]string, 0, len(cfg.PeerURL))
	for id := range cfg.PeerURL {
		peers = append(peers, id)
	}
	tr := transport.NewHTTPTransport(cfg.ID, cfg.PeerURL)
	s.rf = raft.NewRaft(cfg.ID, peers, tr, persister, s.apply)

	mux := http.NewServeMux()
	mux.HandleFunc(transport.PathRequestVote, s.handleRequestVote)
	mux.HandleFunc(transport.PathAppendEntries, s.handleAppendEntries)
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/status", s.handleStatus)
	s.http = &http.Server{Addr: cfg.Addr, Handler: mux}
	return s, nil
}

// Start launches the Raft node and the HTTP server. It blocks until the HTTP
// server stops (or errors).
func (s *Server) Start() error {
	s.rf.Start()
	err := s.http.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Close stops the Raft node and the HTTP server.
func (s *Server) Close() error {
	s.rf.Stop()
	return s.http.Close()
}

// apply is the Raft applyFn: decode the committed entry, mutate the store, and
// publish the new applied index so waiting writers can return.
func (s *Server) apply(msg raft.ApplyMsg) {
	cmd, err := kv.Decode(msg.Command)
	if err == nil {
		s.store.Apply(cmd)
	}
	s.mu.Lock()
	s.appliedIndex = msg.Index
	s.appliedCond.Broadcast()
	s.mu.Unlock()
}

// waitApplied blocks until the given Raft index has been applied to the store,
// or the timeout elapses. This makes a PUT/DELETE synchronous: the client only
// gets 200 once its write is committed and visible.
func (s *Server) waitApplied(index int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.appliedIndex < index {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		// Cond has no timed Wait; a watchdog goroutine broadcasts to bound it.
		timer := time.AfterFunc(remaining, func() {
			s.mu.Lock()
			s.appliedCond.Broadcast()
			s.mu.Unlock()
		})
		s.appliedCond.Wait()
		timer.Stop()
		if time.Now().After(deadline) && s.appliedIndex < index {
			return false
		}
	}
	return true
}

// ---- Internal Raft RPC handlers ----

func (s *Server) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.rf.HandleRequestVote(args))
}

func (s *Server) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, http.StatusOK, s.rf.HandleAppendEntries(args))
}

// ---- Client REST API ----

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := s.rf.Status()
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"id":          st.ID,
		"state":       st.State.String(),
		"term":        st.Term,
		"leader":      st.Leader,
		"commitIndex": st.CommitIndex,
		"lastApplied": st.LastApplied,
		"logLength":   st.LogLength,
	})
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut, http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		s.handleWrite(w, r, kv.Command{Op: kv.OpSet, Key: key, Value: string(body)})
	case http.MethodDelete:
		s.handleWrite(w, r, kv.Command{Op: kv.OpDelete, Key: key})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGet serves a read from the leader's state machine. Reads are
// leader-only: a follower redirects so the client doesn't observe a stale value
// from a node that has fallen behind. (See README "Design decisions".)
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if !s.rf.IsLeader() {
		s.redirectToLeader(w, r)
		return
	}
	value, ok := s.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"key": key, "value": value})
}

// handleWrite submits a mutation through Raft and waits for it to commit+apply.
func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request, cmd kv.Command) {
	data, err := cmd.Encode()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	index, _, isLeader := s.rf.Submit(data)
	if !isLeader {
		s.redirectToLeader(w, r)
		return
	}
	if !s.waitApplied(index, 3*time.Second) {
		http.Error(w, "timed out waiting for commit", http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"key":       cmd.Key,
		"op":        string(cmd.Op),
		"committed": index,
	})
}

// redirectToLeader points the client at the current leader with a 307 Location
// header (including the original request path, so an HTTP client that follows
// redirects lands on the right endpoint) plus a JSON hint body. If the leader
// is unknown (e.g. mid election) it returns 503 so the client retries.
func (s *Server) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	leader := s.rf.Status().Leader
	if leader == "" || leader == s.cfg.ID {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "no leader elected yet, retry shortly",
		})
		return
	}
	base, ok := s.cfg.PeerURL[leader]
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": fmt.Sprintf("leader %s address unknown", leader),
		})
		return
	}
	w.Header().Set("Location", base+r.URL.Path)
	writeJSON(w, http.StatusTemporaryRedirect, map[string]string{
		"error":     "not leader",
		"leader":    leader,
		"leaderURL": base,
	})
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
