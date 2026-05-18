package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/CompileWithG/raftkv/internal/raft"
)

// Raft RPC endpoint paths served by each node's HTTP server.
const (
	PathRequestVote   = "/raft/requestvote"
	PathAppendEntries = "/raft/appendentries"
)

// HTTPTransport implements raft.Transport by POSTing JSON to peers. It maps
// peer node IDs to base URLs (e.g. "http://127.0.0.1:9001").
type HTTPTransport struct {
	self    string
	peerURL map[string]string
	client  *http.Client
}

// NewHTTPTransport builds a transport for `self` given a peerID->baseURL map.
func NewHTTPTransport(self string, peerURL map[string]string) *HTTPTransport {
	return &HTTPTransport{
		self:    self,
		peerURL: peerURL,
		// Timeout kept below the election timeout so a dead peer doesn't wedge
		// the leader's replication loop.
		client: &http.Client{Timeout: 150 * time.Millisecond},
	}
}

func (t *HTTPTransport) SendRequestVote(peer string, args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply
	err := t.post(peer, PathRequestVote, args, &reply)
	return reply, err
}

func (t *HTTPTransport) SendAppendEntries(peer string, args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply
	err := t.post(peer, PathAppendEntries, args, &reply)
	return reply, err
}

func (t *HTTPTransport) post(peer, path string, args, reply interface{}) error {
	base, ok := t.peerURL[peer]
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", peer)
	}
	body, err := json.Marshal(args)
	if err != nil {
		return err
	}
	resp, err := t.client.Post(base+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err // treated as unreachable by the caller
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("transport: peer %q returned %d", peer, resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(reply)
}
