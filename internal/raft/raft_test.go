package raft

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func testConfig(id string, peers []string, tr Transport, st Storage) Config {
	return Config{
		ID:                 id,
		Peers:              peers,
		Transport:          tr,
		Storage:            st,
		ElectionTimeoutMin: 150 * time.Millisecond,
		ElectionTimeoutMax: 300 * time.Millisecond,
		HeartbeatInterval:  30 * time.Millisecond,
		RPCTimeout:         60 * time.Millisecond,
		MaxBatch:           8,
	}
}

type memNet struct {
	mu    sync.Mutex
	nodes map[string]*Raft
	cut   map[[2]string]bool
}

func (n *memNet) node(id string) *Raft {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[id]
}

func (n *memNet) reachable(a, b string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return !n.cut[[2]string{a, b}] && !n.cut[[2]string{b, a}]
}

type memTransport struct {
	net  *memNet
	from string
}

// Dropped requests and dropped replies both look like a timeout to the caller, as on a real network
func (t *memTransport) deliver(ctx context.Context, to string, call func(*Raft) error) error {
	target := t.net.node(to)
	if target == nil || !t.net.reachable(t.from, to) {
		<-ctx.Done()
		return ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- call(target) }()
	select {
	case err := <-done:
		if err == nil && !t.net.reachable(t.from, to) {
			<-ctx.Done()
			return ctx.Err()
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *memTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	return reply, t.deliver(ctx, peer, func(r *Raft) error { return r.HandleRequestVote(args, reply) })
}

func (t *memTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	return reply, t.deliver(ctx, peer, func(r *Raft) error { return r.HandleAppendEntries(args, reply) })
}

type testCluster struct {
	t        *testing.T
	net      *memNet
	ids      []string
	storages map[string]*MemoryStorage

	mu      sync.Mutex
	applied map[string][]string
}

func newTestCluster(t *testing.T, n int) *testCluster {
	c := &testCluster{
		t:        t,
		net:      &memNet{nodes: map[string]*Raft{}, cut: map[[2]string]bool{}},
		storages: map[string]*MemoryStorage{},
		applied:  map[string][]string{},
	}
	for i := range n {
		id := fmt.Sprintf("n%d", i)
		c.ids = append(c.ids, id)
		c.storages[id] = NewMemoryStorage()
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.crash(id)
		}
	})
	return c
}

func (c *testCluster) start(id string) {
	peers := slices.DeleteFunc(slices.Clone(c.ids), func(p string) bool { return p == id })
	c.mu.Lock()
	c.applied[id] = nil
	c.mu.Unlock()
	r, err := New(testConfig(id, peers, &memTransport{net: c.net, from: id}, c.storages[id]), func(m ApplyMsg) {
		if len(m.Command) == 0 {
			return
		}
		c.mu.Lock()
		c.applied[id] = append(c.applied[id], string(m.Command))
		c.mu.Unlock()
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.mu.Lock()
	c.net.nodes[id] = r
	c.net.mu.Unlock()
	r.Start()
}

func (c *testCluster) crash(id string) {
	c.net.mu.Lock()
	r := c.net.nodes[id]
	delete(c.net.nodes, id)
	c.net.mu.Unlock()
	if r != nil {
		r.Stop()
	}
}

func (c *testCluster) isolate(id string) {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	for _, other := range c.ids {
		if other != id {
			c.net.cut[[2]string{id, other}] = true
		}
	}
}

func (c *testCluster) heal() {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	clear(c.net.cut)
}

func (c *testCluster) others(id string) []string {
	return slices.DeleteFunc(slices.Clone(c.ids), func(p string) bool { return p == id })
}

func (c *testCluster) waitLeader(among []string, afterTerm uint64) (string, uint64) {
	c.t.Helper()
	var leader string
	var term uint64
	waitFor(c.t, "a stable leader", 5*time.Second, func() bool {
		leader, term = "", 0
		var maxTerm uint64
		for _, id := range among {
			r := c.net.node(id)
			if r == nil {
				continue
			}
			st := r.Status()
			maxTerm = max(maxTerm, st.Term)
			if st.Role != Leader || st.Term <= afterTerm {
				continue
			}
			if st.Term == term {
				c.t.Fatalf("two leaders in term %d: %s and %s", term, leader, id)
			}
			if st.Term > term {
				leader, term = id, st.Term
			}
		}
		return leader != "" && term == maxTerm
	})
	return leader, term
}

func (c *testCluster) propose(id, cmd string) {
	c.t.Helper()
	if _, _, err := c.net.node(id).Propose([]byte(cmd)); err != nil {
		c.t.Fatalf("propose %q on %s: %v", cmd, id, err)
	}
}

func (c *testCluster) appliedOn(id string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.applied[id])
}

func (c *testCluster) waitApplied(ids []string, want []string) {
	c.t.Helper()
	waitFor(c.t, fmt.Sprintf("%v to apply %v", ids, want), 5*time.Second, func() bool {
		for _, id := range ids {
			if !slices.Equal(c.appliedOn(id), want) {
				return false
			}
		}
		return true
	})
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func commands(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

func TestElectsOneStableLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, term := c.waitLeader(c.ids, 0)
	time.Sleep(600 * time.Millisecond)
	leader2, term2 := c.waitLeader(c.ids, 0)
	if leader != leader2 || term != term2 {
		t.Fatalf("leadership changed without faults: %s@%d -> %s@%d", leader, term, leader2, term2)
	}
}

func TestReplicatesCommandsInOrder(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.waitLeader(c.ids, 0)
	want := commands("cmd", 20)
	for _, cmd := range want {
		c.propose(leader, cmd)
	}
	c.waitApplied(c.ids, want)
}

func TestIsolatedLeaderIsReplacedAndItsUncommittedEntryDiscarded(t *testing.T) {
	c := newTestCluster(t, 3)
	old, oldTerm := c.waitLeader(c.ids, 0)
	c.propose(old, "before")
	c.waitApplied(c.ids, []string{"before"})

	c.isolate(old)
	c.propose(old, "lost")

	majority := c.others(old)
	leader, _ := c.waitLeader(majority, oldTerm)
	c.propose(leader, "after")
	c.waitApplied(majority, []string{"before", "after"})

	if got := c.appliedOn(old); !slices.Equal(got, []string{"before"}) {
		t.Fatalf("isolated old leader applied %v, want only [before]", got)
	}
	if st := c.net.node(old).Status(); st.Role != Leader || st.Term != oldTerm {
		t.Fatalf("isolated old leader should still believe it leads term %d, status %+v", oldTerm, st)
	}

	c.heal()
	c.waitApplied(c.ids, []string{"before", "after"})
	waitFor(t, "old leader to step down", 2*time.Second, func() bool {
		return c.net.node(old).Status().Role == Follower
	})
}

func TestDisconnectedFollowerCatchesUp(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, _ := c.waitLeader(c.ids, 0)
	lagging := c.others(leader)[0]

	c.isolate(lagging)
	want := commands("batch", 30)
	for _, cmd := range want {
		c.propose(leader, cmd)
	}
	c.waitApplied(c.others(lagging), want)
	if n := len(c.appliedOn(lagging)); n != 0 {
		t.Fatalf("isolated follower applied %d entries", n)
	}

	c.heal()
	c.waitApplied(c.ids, want)
}

func TestFullClusterRestartRecoversFromStorage(t *testing.T) {
	c := newTestCluster(t, 3)
	leader, term := c.waitLeader(c.ids, 0)
	want := commands("durable", 5)
	for _, cmd := range want {
		c.propose(leader, cmd)
	}
	c.waitApplied(c.ids, want)

	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	c.waitLeader(c.ids, term)
	c.waitApplied(c.ids, want)
}

func newUnitRaft(t *testing.T, id string, peers []string, terms ...uint64) (*Raft, *MemoryStorage) {
	t.Helper()
	st := NewMemoryStorage()
	var entries []Entry
	for _, term := range terms {
		entries = append(entries, Entry{Term: term})
	}
	if len(entries) > 0 {
		if err := st.AppendEntries(1, entries); err != nil {
			t.Fatal(err)
		}
	}
	r, err := New(testConfig(id, peers, nil, st), func(ApplyMsg) {})
	if err != nil {
		t.Fatal(err)
	}
	return r, st
}

func logTerms(entries []Entry) []uint64 {
	out := make([]uint64, len(entries))
	for i, e := range entries {
		out[i] = e.Term
	}
	return out
}

func TestCommitRuleIgnoresReplicatedEntriesFromEarlierTerms(t *testing.T) {
	r, _ := newUnitRaft(t, "l", []string{"a", "b"}, 1, 2)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentTerm = 3
	r.role = Leader
	r.matchIndex["a"], r.matchIndex["b"] = 2, 2

	r.advanceCommitLocked()
	if r.commitIndex != 0 {
		t.Fatalf("committed index %d from an earlier term by counting replicas", r.commitIndex)
	}

	r.log = append(r.log, Entry{Term: 3})
	r.matchIndex["a"] = 3
	r.advanceCommitLocked()
	if r.commitIndex != 3 {
		t.Fatalf("commitIndex = %d, want 3 once a current-term entry reaches a majority", r.commitIndex)
	}
}

func TestHandleAppendEntries(t *testing.T) {
	r, st := newUnitRaft(t, "f", []string{"l"}, 1, 1, 2, 2)
	r.currentTerm = 2

	steps := []struct {
		name       string
		args       AppendEntriesArgs
		want       AppendEntriesReply
		wantTerms  []uint64
		wantCommit uint64
	}{
		{"prev term mismatch", AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 3},
			AppendEntriesReply{Term: 2, ConflictTerm: 2, ConflictIndex: 3}, []uint64{1, 1, 2, 2}, 0},
		{"prev beyond log", AppendEntriesArgs{Term: 2, LeaderID: "l", PrevLogIndex: 6, PrevLogTerm: 2},
			AppendEntriesReply{Term: 2, ConflictIndex: 5}, []uint64{1, 1, 2, 2}, 0},
		{"conflict truncates", AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 2, PrevLogTerm: 1, Entries: []Entry{{Term: 3}, {Term: 3}}, LeaderCommit: 3},
			AppendEntriesReply{Term: 3, Success: true}, []uint64{1, 1, 3, 3}, 3},
		{"stale duplicate keeps suffix", AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{{Term: 1}}, LeaderCommit: 1},
			AppendEntriesReply{Term: 3, Success: true}, []uint64{1, 1, 3, 3}, 3},
		{"older term rejected", AppendEntriesArgs{Term: 2, LeaderID: "old", PrevLogIndex: 4, PrevLogTerm: 3, LeaderCommit: 4},
			AppendEntriesReply{Term: 3}, []uint64{1, 1, 3, 3}, 3},
		{"commit capped at last new entry", AppendEntriesArgs{Term: 3, LeaderID: "l", PrevLogIndex: 4, PrevLogTerm: 3, LeaderCommit: 10},
			AppendEntriesReply{Term: 3, Success: true}, []uint64{1, 1, 3, 3}, 4},
	}
	for _, s := range steps {
		var reply AppendEntriesReply
		if err := r.HandleAppendEntries(&s.args, &reply); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if reply != s.want {
			t.Errorf("%s: reply %+v, want %+v", s.name, reply, s.want)
		}
		if got := logTerms(r.log[1:]); !slices.Equal(got, s.wantTerms) {
			t.Errorf("%s: log terms %v, want %v", s.name, got, s.wantTerms)
		}
		if r.commitIndex != s.wantCommit {
			t.Errorf("%s: commitIndex %d, want %d", s.name, r.commitIndex, s.wantCommit)
		}
	}

	hs, entries, _ := st.Load()
	if hs.Term != 3 || !slices.Equal(logTerms(entries), []uint64{1, 1, 3, 3}) {
		t.Errorf("persisted term %d log %v, want term 3 log [1 1 3 3]", hs.Term, logTerms(entries))
	}
}

func TestHandleRequestVote(t *testing.T) {
	r, st := newUnitRaft(t, "v", []string{"x", "y", "z", "w"}, 1, 2)
	r.currentTerm = 2

	steps := []struct {
		name string
		args RequestVoteArgs
		want RequestVoteReply
	}{
		{"older last term denied", RequestVoteArgs{Term: 3, CandidateID: "x", LastLogIndex: 5, LastLogTerm: 1}, RequestVoteReply{Term: 3}},
		{"shorter log denied", RequestVoteArgs{Term: 3, CandidateID: "y", LastLogIndex: 1, LastLogTerm: 2}, RequestVoteReply{Term: 3}},
		{"up to date granted", RequestVoteArgs{Term: 3, CandidateID: "x", LastLogIndex: 2, LastLogTerm: 2}, RequestVoteReply{Term: 3, VoteGranted: true}},
		{"second candidate denied", RequestVoteArgs{Term: 3, CandidateID: "z", LastLogIndex: 9, LastLogTerm: 3}, RequestVoteReply{Term: 3}},
		{"same candidate regranted", RequestVoteArgs{Term: 3, CandidateID: "x", LastLogIndex: 2, LastLogTerm: 2}, RequestVoteReply{Term: 3, VoteGranted: true}},
		{"stale term denied", RequestVoteArgs{Term: 2, CandidateID: "w", LastLogIndex: 9, LastLogTerm: 9}, RequestVoteReply{Term: 3}},
	}
	for _, s := range steps {
		var reply RequestVoteReply
		if err := r.HandleRequestVote(&s.args, &reply); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if reply != s.want {
			t.Errorf("%s: reply %+v, want %+v", s.name, reply, s.want)
		}
	}
	if hs, _, _ := st.Load(); hs != (HardState{Term: 3, VotedFor: "x"}) {
		t.Errorf("persisted hard state %+v, want term 3 vote x", hs)
	}
}
