package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// Timing parameters. The election timeout is randomized in
// [electionTimeoutMin, electionTimeoutMax) so that split votes are rare and
// resolved quickly. The heartbeat interval must be well below the election
// timeout so a live leader keeps followers from timing out.
const (
	electionTimeoutMin = 250 * time.Millisecond
	electionTimeoutMax = 400 * time.Millisecond
	heartbeatInterval  = 60 * time.Millisecond
	tickInterval       = 15 * time.Millisecond
)

// Raft is a single node in the cluster. All mutable fields are guarded by mu.
type Raft struct {
	mu        sync.Mutex
	id        string
	peers     []string // IDs of the OTHER nodes (never includes self)
	transport Transport
	persister Persister
	applyFn   func(ApplyMsg) // called (without mu held) for each committed entry

	// --- Persistent state (survives restart; written via persist()) ---
	currentTerm int
	votedFor    string  // candidate ID that received our vote this term ("" = none)
	log         []Entry // 1-based; log[0] is a term-0 sentinel so indices line up

	// --- Volatile state on all servers ---
	state       State
	commitIndex int    // highest log index known to be committed
	lastApplied int    // highest log index applied to the state machine
	leaderID    string // last known leader, for client redirects

	// --- Volatile state on leaders (reset on each election) ---
	nextIndex  map[string]int // for each peer, next log index to send
	matchIndex map[string]int // for each peer, highest index known replicated

	// Election timer: the election fires if now - electionResetEvent exceeds a
	// randomized timeout. Any valid heartbeat / granted vote resets it.
	electionResetEvent time.Time
	electionTimeout    time.Duration

	applyCond *sync.Cond // signalled when commitIndex advances
	dead      int32      // set by Stop(); checked by background loops
}

// NewRaft constructs a node and restores any persisted state, but does not yet
// run the background loops — call Start() for that. applyFn is invoked once,
// in log order, for every entry that becomes committed.
func NewRaft(id string, peers []string, transport Transport, persister Persister, applyFn func(ApplyMsg)) *Raft {
	rf := &Raft{
		id:         id,
		peers:      peers,
		transport:  transport,
		persister:  persister,
		applyFn:    applyFn,
		state:      Follower,
		votedFor:   "",
		log:        []Entry{{Term: 0}}, // sentinel at index 0
		nextIndex:  make(map[string]int),
		matchIndex: make(map[string]int),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.loadPersisted()
	rf.resetElectionTimer()
	return rf
}

// Start launches the election ticker and the applier goroutine.
func (rf *Raft) Start() {
	go rf.ticker()
	go rf.applier()
}

// Stop halts all background activity. It is safe to call once.
func (rf *Raft) Stop() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast() // wake the applier so it can exit
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool { return atomic.LoadInt32(&rf.dead) == 1 }

// --- Small log helpers. Callers must hold rf.mu. ---

func (rf *Raft) lastIndex() int { return len(rf.log) - 1 }

func (rf *Raft) lastTerm() int { return rf.log[len(rf.log)-1].Term }

func (rf *Raft) quorum() int { return (len(rf.peers)+1)/2 + 1 }

func (rf *Raft) resetElectionTimer() {
	rf.electionResetEvent = time.Now()
	d := electionTimeoutMax - electionTimeoutMin
	rf.electionTimeout = electionTimeoutMin + time.Duration(rand.Int63n(int64(d)))
}

// ------------------------------------------------------------------
// Election
// ------------------------------------------------------------------

// ticker is the master timer loop. For non-leaders it starts an election once
// the randomized election timeout elapses without contact from a leader.
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(tickInterval)
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.electionResetEvent) >= rf.electionTimeout {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection transitions to Candidate, votes for self, and solicits votes
// from every peer. Must be called with rf.mu held. Reply handling runs in
// separate goroutines that re-acquire the lock.
func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.id
	rf.resetElectionTimer()
	rf.persist()

	term := rf.currentTerm
	lastLogIndex := rf.lastIndex()
	lastLogTerm := rf.lastTerm()
	votes := 1 // vote for self

	for _, peer := range rf.peers {
		go func(peer string) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply, err := rf.transport.SendRequestVote(peer, args)
			if err != nil {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// A higher term anywhere forces us back to Follower.
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			// Ignore stale replies (term moved on, or we already changed role).
			if rf.state != Candidate || rf.currentTerm != term {
				return
			}
			if reply.VoteGranted {
				votes++
				if votes >= rf.quorum() {
					rf.becomeLeader()
				}
			}
		}(peer)
	}
}

// becomeFollower steps down (or stays) as Follower, adopting term. Resetting
// votedFor when the term increases is what lets us vote in the new term. Must
// hold rf.mu.
func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = ""
	}
	rf.persist()
}

// becomeLeader initializes leader bookkeeping and starts the replication loop.
// Must hold rf.mu. nextIndex optimistically starts just past our log; failed
// AppendEntries walk it back to the point of agreement.
func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	rf.leaderID = rf.id

	// Append a no-op entry in our own term. The §5.4.2 commit restriction
	// forbids a new leader from marking prior-term entries committed until it
	// has committed an entry from its current term. Committing this no-op does
	// exactly that, so writes accepted by the previous leader become
	// committed/applied promptly (and readable) instead of waiting for the next
	// client write. A nil command is ignored by the state machine.
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: nil})
	rf.persist()

	for _, peer := range rf.peers {
		rf.nextIndex[peer] = rf.lastIndex() + 1
		rf.matchIndex[peer] = 0
	}
	go rf.leaderLoop(rf.currentTerm)
}

// ------------------------------------------------------------------
// Log replication (leader side)
// ------------------------------------------------------------------

// leaderLoop broadcasts AppendEntries (heartbeats + new entries) at a steady
// cadence for as long as we remain leader in `term`.
func (rf *Raft) leaderLoop(term int) {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		rf.broadcastAppendEntries(term)
		time.Sleep(heartbeatInterval)
	}
}

func (rf *Raft) broadcastAppendEntries(term int) {
	for _, peer := range rf.peers {
		go rf.replicateTo(peer, term)
	}
}

// replicateTo sends a single AppendEntries to one peer and processes the reply:
// on success it advances matchIndex/nextIndex and re-checks the commit index;
// on failure it walks nextIndex back using the follower's conflict hint.
func (rf *Raft) replicateTo(peer string, term int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	ni := rf.nextIndex[peer]
	if ni < 1 {
		ni = 1
	}
	prevLogIndex := ni - 1
	prevLogTerm := rf.log[prevLogIndex].Term
	entries := append([]Entry(nil), rf.log[ni:]...) // copy to avoid races on the log slice
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     rf.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply, err := rf.transport.SendAppendEntries(peer, args)
	if err != nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if rf.state != Leader || rf.currentTerm != term {
		return // stale reply
	}

	if reply.Success {
		match := prevLogIndex + len(entries)
		if match > rf.matchIndex[peer] {
			rf.matchIndex[peer] = match
			rf.nextIndex[peer] = match + 1
		}
		rf.advanceCommit()
		return
	}

	// Rejected: use the conflict hint to skip past the whole conflicting term
	// in one step instead of decrementing nextIndex entry-by-entry.
	if reply.ConflictTerm > 0 {
		// Does the leader have any entry with ConflictTerm? If so, resume just
		// after the last one; otherwise back up to the follower's first
		// conflicting index.
		lastIdxOfTerm := 0
		for i := rf.lastIndex(); i >= 1; i-- {
			if rf.log[i].Term == reply.ConflictTerm {
				lastIdxOfTerm = i
				break
			}
		}
		if lastIdxOfTerm > 0 {
			rf.nextIndex[peer] = lastIdxOfTerm + 1
		} else {
			rf.nextIndex[peer] = reply.ConflictIndex
		}
	} else {
		rf.nextIndex[peer] = reply.ConflictIndex
	}
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1
	}
}

// advanceCommit finds the highest index N such that a majority of matchIndex
// values are >= N and log[N] is from the current term, then commits up to N.
// Requiring log[N].Term == currentTerm is the subtle safety rule from paper
// §5.4.2: a leader may not commit an entry from a previous term just because it
// is stored on a majority — it becomes committed only once an entry from the
// leader's own term is also replicated on a majority. Must hold rf.mu.
func (rf *Raft) advanceCommit() {
	for n := rf.lastIndex(); n > rf.commitIndex; n-- {
		if rf.log[n].Term != rf.currentTerm {
			continue
		}
		count := 1 // the leader itself has entry n
		for _, peer := range rf.peers {
			if rf.matchIndex[peer] >= n {
				count++
			}
		}
		if count >= rf.quorum() {
			rf.commitIndex = n
			rf.applyCond.Signal()
			return
		}
	}
}

// ------------------------------------------------------------------
// RPC handlers (follower/receiver side)
// ------------------------------------------------------------------

// HandleRequestVote processes an incoming RequestVote RPC (paper §5.2, §5.4.1).
func (rf *Raft) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply := RequestVoteReply{Term: rf.currentTerm, VoteGranted: false}
	if args.Term < rf.currentTerm {
		return reply // reject: candidate is behind
	}

	// Election restriction (§5.4.1): only grant the vote if the candidate's log
	// is at least as up-to-date as ours. "More up-to-date" = higher last term,
	// or equal last term with an equal-or-longer log. This guarantees the new
	// leader holds every committed entry.
	upToDate := args.LastLogTerm > rf.lastTerm() ||
		(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex())

	if (rf.votedFor == "" || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		rf.persist()
		rf.resetElectionTimer() // granting a vote counts as hearing from a leader-to-be
		reply.VoteGranted = true
	}
	return reply
}

// HandleAppendEntries processes an incoming AppendEntries RPC (paper §5.3):
// heartbeat handling, the log-matching consistency check with a conflict hint,
// conflict truncation, appending new entries, and advancing the commit index.
func (rf *Raft) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := AppendEntriesReply{Term: rf.currentTerm, Success: false}
	if args.Term < rf.currentTerm {
		return reply // reject: stale leader
	}

	// Valid leader for this (>=) term: adopt term if newer and become Follower.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = ""
	}
	rf.state = Follower
	rf.leaderID = args.LeaderID
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	// Consistency check: we must have an entry at PrevLogIndex whose term
	// matches PrevLogTerm, otherwise our logs have diverged before this point.
	if args.PrevLogIndex > rf.lastIndex() {
		// Our log is too short. Tell the leader to back up to our end.
		reply.ConflictIndex = rf.lastIndex() + 1
		reply.ConflictTerm = 0
		return reply
	}
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// Term mismatch at PrevLogIndex. Report the conflicting term and the
		// first index we hold for it, so the leader can skip the whole term.
		ct := rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for i > 1 && rf.log[i-1].Term == ct {
			i--
		}
		reply.ConflictTerm = ct
		reply.ConflictIndex = i
		return reply
	}

	// Logs agree up to PrevLogIndex. Merge in the new entries, truncating only
	// on a genuine term conflict — never blindly, because a delayed/duplicate
	// AppendEntries must not chop off entries we have already committed.
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= rf.lastIndex() {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx] // drop the conflict and everything after it
				rf.log = append(rf.log, entry)
			}
			// else: identical entry already present, keep it
		} else {
			rf.log = append(rf.log, entry)
		}
	}
	rf.persist()

	// Adopt the leader's commit index, bounded by what we actually store.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIndex())
		rf.applyCond.Signal()
	}

	reply.Success = true
	return reply
}

// ------------------------------------------------------------------
// Apply loop + client-facing API
// ------------------------------------------------------------------

// applier delivers committed-but-not-yet-applied entries to the state machine,
// strictly in log order. It releases the lock while calling applyFn so the
// state machine can call back into Raft without deadlocking.
func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied].Command}
			rf.mu.Unlock()
			rf.applyFn(msg)
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}

// Submit appends a command to the leader's log and starts replication. It
// returns the index the command will occupy if committed, the current term, and
// whether this node is the leader. A non-leader returns isLeader=false and the
// caller should redirect to Status().Leader.
func (rf *Raft) Submit(command []byte) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return 0, rf.currentTerm, false
	}
	rf.log = append(rf.log, Entry{Term: rf.currentTerm, Command: command})
	rf.persist()
	index = rf.lastIndex()
	term = rf.currentTerm
	go rf.broadcastAppendEntries(term) // push it out now instead of waiting for the next heartbeat
	return index, term, true
}

// Status is an immutable snapshot of the node's state for the /status endpoint
// and for tests.
type Status struct {
	ID          string
	State       State
	Term        int
	Leader      string
	CommitIndex int
	LastApplied int
	LogLength   int // number of real entries (excludes the sentinel)
}

func (rf *Raft) Status() Status {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return Status{
		ID:          rf.id,
		State:       rf.state,
		Term:        rf.currentTerm,
		Leader:      rf.leaderID,
		CommitIndex: rf.commitIndex,
		LastApplied: rf.lastApplied,
		LogLength:   rf.lastIndex(),
	}
}

// IsLeader reports whether this node currently believes it is the leader.
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}
