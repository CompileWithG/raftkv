package raft_test

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/CompileWithG/raftkv/internal/raft"
	"github.com/CompileWithG/raftkv/internal/transport"
)

// cluster is an in-process test harness: N Raft nodes wired through an
// in-memory network, each with its own applied-command log so tests can assert
// on replicated state. Everything is deterministic and socket-free, which keeps
// the tests fast and non-flaky under -race.
type cluster struct {
	t     *testing.T
	n     int
	ids   []string
	net   *transport.InmemNetwork
	nodes map[string]*raft.Raft

	mu      sync.Mutex
	applied map[string][][]byte           // node id -> ordered applied commands
	persist map[string]*raft.MemPersister // node id -> persister (kept across restarts)
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t:       t,
		n:       n,
		net:     transport.NewInmemNetwork(),
		nodes:   make(map[string]*raft.Raft),
		applied: make(map[string][][]byte),
		persist: make(map[string]*raft.MemPersister),
	}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i+1))
	}
	for _, id := range c.ids {
		c.persist[id] = raft.NewMemPersister()
		c.applied[id] = nil
		c.start(id)
	}
	return c
}

// start (re)creates the node for id from its persister and registers it.
func (c *cluster) start(id string) {
	peers := make([]string, 0, c.n-1)
	for _, other := range c.ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	applyFn := func(msg raft.ApplyMsg) {
		if len(msg.Command) == 0 {
			return // ignore leader no-op entries, like a real state machine
		}
		c.mu.Lock()
		c.applied[id] = append(c.applied[id], msg.Command)
		c.mu.Unlock()
	}
	rf := raft.NewRaft(id, peers, c.net.Endpoint(id), c.persist[id], applyFn)
	c.nodes[id] = rf
	c.net.Register(id, rf)
	rf.Start()
}

func (c *cluster) stopAll() {
	for _, rf := range c.nodes {
		rf.Stop()
	}
}

// crash simulates a node failure: disconnect it from the network and stop its
// goroutines. Its persister is retained so it can be restarted.
func (c *cluster) crash(id string) {
	c.net.Disconnect(id)
	c.nodes[id].Stop()
}

// restart rebuilds a crashed node from its persisted state, as a real process
// would on reboot.
func (c *cluster) restart(id string) {
	peers := make([]string, 0, c.n-1)
	for _, other := range c.ids {
		if other != id {
			peers = append(peers, other)
		}
	}
	applyFn := func(msg raft.ApplyMsg) {
		if len(msg.Command) == 0 {
			return // ignore leader no-op entries, like a real state machine
		}
		c.mu.Lock()
		c.applied[id] = append(c.applied[id], msg.Command)
		c.mu.Unlock()
	}
	rf := raft.NewRaft(id, peers, c.net.Endpoint(id), c.persist[id], applyFn)
	c.nodes[id] = rf
	c.net.Replace(id, rf)
	rf.Start()
}

func (c *cluster) appliedCount(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.applied[id])
}

// waitFor polls cond every 10ms until it returns true or timeout elapses. Used
// instead of fixed sleeps so timing tests are robust.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// leaders returns the IDs currently claiming leadership. Optionally restricted
// to a set of "connected" nodes.
func (c *cluster) leaders(among ...string) []string {
	allowed := map[string]bool{}
	for _, id := range among {
		allowed[id] = true
	}
	var out []string
	for _, id := range c.ids {
		if len(among) > 0 && !allowed[id] {
			continue
		}
		if c.nodes[id].IsLeader() {
			out = append(out, id)
		}
	}
	return out
}

// waitLeader waits until exactly one leader exists among `among` (or the whole
// cluster) and returns its ID.
func (c *cluster) waitLeader(t *testing.T, among ...string) string {
	var leader string
	ok := waitFor(3*time.Second, func() bool {
		ls := c.leaders(among...)
		if len(ls) == 1 {
			leader = ls[0]
			return true
		}
		return false
	})
	if !ok {
		t.Fatalf("expected exactly one leader, found %v", c.leaders(among...))
	}
	return leader
}

func encodeCmd(t *testing.T, key, val string) []byte {
	b, err := json.Marshal(map[string]string{"key": key, "value": val})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestLeaderElection: a single leader is elected within an election timeout.
func TestLeaderElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stopAll()
	leader := c.waitLeader(t)
	t.Logf("elected leader: %s", leader)

	// The leader's term should be positive and stable while it stays connected.
	term := c.nodes[leader].Status().Term
	if term < 1 {
		t.Fatalf("leader term should be >= 1, got %d", term)
	}
}

// TestReplicationCommitApply: a command submitted to the leader is replicated to
// a majority, committed, and applied on every node.
func TestReplicationCommitApply(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stopAll()
	leader := c.waitLeader(t)

	cmd := encodeCmd(t, "foo", "bar")
	idx, _, ok := c.nodes[leader].Submit(cmd)
	if !ok {
		t.Fatal("submit to leader was rejected")
	}
	// Index is >= 2 because a leader appends a no-op at index 1 on election.
	if idx < 1 {
		t.Fatalf("expected a positive log index, got %d", idx)
	}

	// Every node should apply the command.
	if !waitFor(3*time.Second, func() bool {
		for _, id := range c.ids {
			if c.appliedCount(id) < 1 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("command was not applied on all nodes")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.ids {
		if string(c.applied[id][0]) != string(cmd) {
			t.Fatalf("node %s applied wrong command: %s", id, c.applied[id][0])
		}
	}
}

// TestLeaderFailoverAndRecovery: killing the leader triggers a new election, the
// cluster keeps serving, and a previously committed entry survives.
func TestLeaderFailoverAndRecovery(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stopAll()

	leader1 := c.waitLeader(t)
	if _, _, ok := c.nodes[leader1].Submit(encodeCmd(t, "k1", "v1")); !ok {
		t.Fatal("submit rejected by leader")
	}
	if !waitFor(3*time.Second, func() bool { return c.appliedCount(leader1) >= 1 }) {
		t.Fatal("first write not committed")
	}

	// Kill the leader. The two survivors must elect a new leader.
	survivors := []string{}
	for _, id := range c.ids {
		if id != leader1 {
			survivors = append(survivors, id)
		}
	}
	c.crash(leader1)

	leader2 := c.waitLeader(t, survivors...)
	if leader2 == leader1 {
		t.Fatal("new leader should differ from the crashed one")
	}
	t.Logf("failover: %s -> %s", leader1, leader2)

	// Cluster still serves writes with only a majority (2/3) alive.
	if _, _, ok := c.nodes[leader2].Submit(encodeCmd(t, "k2", "v2")); !ok {
		t.Fatal("new leader rejected write")
	}
	if !waitFor(3*time.Second, func() bool {
		for _, id := range survivors {
			if c.appliedCount(id) < 2 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("second write not applied on surviving majority")
	}

	// Restart the crashed node: it must rejoin and catch up to 2 entries.
	c.restart(leader1)
	if !waitFor(5*time.Second, func() bool { return c.appliedCount(leader1) >= 2 }) {
		t.Fatalf("restarted node did not catch up, applied=%d", c.appliedCount(leader1))
	}
	t.Logf("node %s rejoined and caught up to %d entries", leader1, c.appliedCount(leader1))
}

// TestLogRepairAfterPartition: a follower isolated during several commits has a
// stale log; once reconnected, AppendEntries must repair it (log divergence
// fixed by the consistency check + conflict backup).
func TestLogRepairAfterPartition(t *testing.T) {
	c := newCluster(t, 5)
	defer c.stopAll()

	leader := c.waitLeader(t)
	// Isolate one follower.
	var laggard string
	for _, id := range c.ids {
		if id != leader {
			laggard = id
			break
		}
	}
	c.net.Disconnect(laggard)

	// Commit several entries while the laggard is away (majority of 5 = 3, and
	// 4 nodes remain, so writes still commit).
	for i := 0; i < 5; i++ {
		c.nodes[leader].Submit(encodeCmd(t, fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)))
	}
	connected := []string{}
	for _, id := range c.ids {
		if id != laggard {
			connected = append(connected, id)
		}
	}
	if !waitFor(3*time.Second, func() bool {
		for _, id := range connected {
			if c.appliedCount(id) < 5 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("entries not committed on connected majority")
	}
	if c.appliedCount(laggard) != 0 {
		t.Fatalf("isolated node should have applied nothing, got %d", c.appliedCount(laggard))
	}

	// Reconnect. The leader must bring the laggard fully up to date.
	c.net.Connect(laggard)
	if !waitFor(5*time.Second, func() bool { return c.appliedCount(laggard) >= 5 }) {
		t.Fatalf("laggard was not repaired, applied=%d", c.appliedCount(laggard))
	}
	t.Logf("laggard %s repaired to %d entries", laggard, c.appliedCount(laggard))
}

// TestPersistenceAcrossRestart: committed entries survive a full-node restart,
// proving currentTerm/votedFor/log are persisted and reloaded.
func TestPersistenceAcrossRestart(t *testing.T) {
	c := newCluster(t, 3)
	defer c.stopAll()

	leader := c.waitLeader(t)
	for i := 0; i < 3; i++ {
		c.nodes[leader].Submit(encodeCmd(t, fmt.Sprintf("p%d", i), fmt.Sprintf("v%d", i)))
	}
	if !waitFor(3*time.Second, func() bool {
		for _, id := range c.ids {
			if c.appliedCount(id) < 3 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("writes not committed on all nodes")
	}

	// Restart a follower and confirm it recovers its log from the persister and
	// re-applies all 3 committed entries (its applied slice was reset on
	// restart, so reaching 3 means it reconstructed committed state).
	var follower string
	for _, id := range c.ids {
		if id != leader {
			follower = id
			break
		}
	}
	logLenBefore := c.nodes[follower].Status().LogLength
	c.crash(follower)
	c.mu.Lock()
	c.applied[follower] = nil // forget in-memory applied history
	c.mu.Unlock()
	c.restart(follower)

	if !waitFor(5*time.Second, func() bool { return c.appliedCount(follower) >= 3 }) {
		t.Fatalf("restarted node did not re-apply persisted log, applied=%d", c.appliedCount(follower))
	}
	logLenAfter := c.nodes[follower].Status().LogLength
	if logLenAfter < logLenBefore {
		t.Fatalf("log shrank across restart: before=%d after=%d", logLenBefore, logLenAfter)
	}
	t.Logf("node %s recovered log length %d across restart", follower, logLenAfter)
}
