// Package raft contains a from-scratch implementation of the Raft consensus
// algorithm (Ongaro & Ousterhout, "In Search of an Understandable Consensus
// Algorithm", extended version). It implements leader election, log
// replication, the safety rules (election restriction, log matching with
// conflict truncation), crash-safe persistence, and applying committed entries
// to a user-supplied state machine.
//
// The package is transport-agnostic: it talks to peers through the Transport
// interface, so the same core is exercised both by an in-memory network in the
// tests and by an HTTP/JSON transport in the running server.
package raft

// State is the role a node currently plays in the cluster.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Entry is a single record in the replicated log. Command is an opaque byte
// slice; the Raft core never interprets it — the state machine (here the KV
// store) encodes and decodes it. Term is the leader's term when the entry was
// created and is central to the log-matching safety property.
type Entry struct {
	Term    int    `json:"term"`
	Command []byte `json:"command"`
}

// ApplyMsg is handed to the state machine when an entry is known to be
// committed. Index is the 1-based position of the entry in the log.
type ApplyMsg struct {
	Index   int
	Command []byte
}

// RequestVoteArgs are the arguments for the RequestVote RPC (paper §5.2, §5.4).
type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

// RequestVoteReply is the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// AppendEntriesArgs are the arguments for the AppendEntries RPC (paper §5.3).
// An empty Entries slice is a heartbeat.
type AppendEntriesArgs struct {
	Term         int     `json:"term"`
	LeaderID     string  `json:"leaderId"`
	PrevLogIndex int     `json:"prevLogIndex"`
	PrevLogTerm  int     `json:"prevLogTerm"`
	Entries      []Entry `json:"entries"`
	LeaderCommit int     `json:"leaderCommit"`
}

// AppendEntriesReply is the reply for the AppendEntries RPC. ConflictIndex and
// ConflictTerm implement the fast-backup optimization described in the paper's
// discussion (§5.3): instead of decrementing nextIndex one entry per round
// trip, the follower tells the leader where its log actually diverges.
type AppendEntriesReply struct {
	Term          int  `json:"term"`
	Success       bool `json:"success"`
	ConflictIndex int  `json:"conflictIndex"`
	ConflictTerm  int  `json:"conflictTerm"`
}

// Transport lets a Raft node send RPCs to a peer identified by its node ID.
// Implementations return an error only for transport-level failures (peer
// unreachable, partitioned); a delivered RPC always yields a reply.
type Transport interface {
	SendRequestVote(peerID string, args RequestVoteArgs) (RequestVoteReply, error)
	SendAppendEntries(peerID string, args AppendEntriesArgs) (AppendEntriesReply, error)
}
