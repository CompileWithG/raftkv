// Package transport provides the wire between Raft nodes. It ships two
// implementations of raft.Transport: an in-memory network used by the tests
// (fast, deterministic, and able to simulate partitions) and an HTTP/JSON
// transport used by the real multi-process cluster.
package transport

import (
	"errors"
	"sync"

	"github.com/CompileWithG/raftkv/internal/raft"
)

// ErrUnreachable models a dropped/partitioned RPC.
var ErrUnreachable = errors.New("transport: peer unreachable")

// InmemNetwork wires a set of in-process Raft nodes together. Tests use
// Disconnect/Connect to simulate a node crashing or a network partition without
// any real sockets, which makes them fast and free of flakiness.
type InmemNetwork struct {
	mu        sync.RWMutex
	nodes     map[string]*raft.Raft
	connected map[string]bool
}

func NewInmemNetwork() *InmemNetwork {
	return &InmemNetwork{
		nodes:     make(map[string]*raft.Raft),
		connected: make(map[string]bool),
	}
}

// Register attaches a live node under its ID (starts connected).
func (n *InmemNetwork) Register(id string, rf *raft.Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = rf
	n.connected[id] = true
}

// Disconnect isolates a node: RPCs to and from it fail until Connect. This
// models both a crash and a network partition.
func (n *InmemNetwork) Disconnect(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = false
}

// Connect reattaches a previously disconnected node.
func (n *InmemNetwork) Connect(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.connected[id] = true
}

// Replace swaps in a new node instance for an existing ID (used by the restart
// test) and marks it connected.
func (n *InmemNetwork) Replace(id string, rf *raft.Raft) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = rf
	n.connected[id] = true
}

// Endpoint returns a raft.Transport bound to the caller's node ID. It can be
// created before the nodes are registered because lookups happen lazily at call
// time.
func (n *InmemNetwork) Endpoint(from string) raft.Transport {
	return &inmemEndpoint{net: n, from: from}
}

func (n *InmemNetwork) route(from, to string) (*raft.Raft, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !n.connected[from] || !n.connected[to] {
		return nil, false
	}
	rf, ok := n.nodes[to]
	return rf, ok
}

type inmemEndpoint struct {
	net  *InmemNetwork
	from string
}

func (e *inmemEndpoint) SendRequestVote(peer string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	target, ok := e.net.route(e.from, peer)
	if !ok {
		return raft.RequestVoteReply{}, ErrUnreachable
	}
	return target.HandleRequestVote(args), nil
}

func (e *inmemEndpoint) SendAppendEntries(peer string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	target, ok := e.net.route(e.from, peer)
	if !ok {
		return raft.AppendEntriesReply{}, ErrUnreachable
	}
	return target.HandleAppendEntries(args), nil
}
