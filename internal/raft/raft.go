package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"sync"
	"time"
)

type Role string

const (
	Follower  Role = "follower"
	Candidate Role = "candidate"
	Leader    Role = "leader"
)

type Entry struct {
	Term    uint64
	Command []byte
}

type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

type Status struct {
	ID           string
	Role         Role
	Term         uint64
	LeaderID     string
	CommitIndex  uint64
	LastApplied  uint64
	LastLogIndex uint64
}

type NotLeaderError struct {
	LeaderID string
}

func (e *NotLeaderError) Error() string {
	if e.LeaderID == "" {
		return "raft: not leader, leader unknown"
	}
	return "raft: not leader, leader is " + e.LeaderID
}

var ErrStopped = errors.New("raft: stopped")

type Config struct {
	ID                 string
	Peers              []string
	Transport          Transport
	Storage            Storage
	Logger             *log.Logger
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
	RPCTimeout         time.Duration
	MaxBatch           int
}

func (c *Config) setDefaults() {
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 300 * time.Millisecond
	}
	if c.ElectionTimeoutMax <= c.ElectionTimeoutMin {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 100 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 250 * time.Millisecond
	}
	if c.MaxBatch == 0 {
		c.MaxBatch = 256
	}
	if c.Logger == nil {
		c.Logger = log.New(io.Discard, "", 0)
	}
}

type Raft struct {
	cfg   Config
	apply func(ApplyMsg)

	mu               sync.Mutex
	applyCond        *sync.Cond
	currentTerm      uint64
	votedFor         string
	log              []Entry
	role             Role
	leaderID         string
	commitIndex      uint64
	lastApplied      uint64
	nextIndex        map[string]uint64
	matchIndex       map[string]uint64
	electionDeadline time.Time
	stopped          bool

	triggers map[string]chan struct{}
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

/** New restores persisted state but starts no goroutines, so the caller can
    finish wiring the apply callback before Start
**/
func New(cfg Config, apply func(ApplyMsg)) (*Raft, error) {
	cfg.setDefaults()
	hs, entries, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: load storage: %w", err)
	}
	r := &Raft{
		cfg:         cfg,
		apply:       apply,
		currentTerm: hs.Term,
		votedFor:    hs.VotedFor,
		log:         append([]Entry{{}}, entries...),
		role:        Follower,
		nextIndex:   make(map[string]uint64),
		matchIndex:  make(map[string]uint64),
		triggers:    make(map[string]chan struct{}),
		stopCh:      make(chan struct{}),
	}
	r.applyCond = sync.NewCond(&r.mu)
	for _, p := range cfg.Peers {
		r.triggers[p] = make(chan struct{}, 1)
	}
	return r, nil
}

func (r *Raft) Start() {
	r.mu.Lock()
	r.resetElectionTimerLocked()
	r.logf("starting: term %d, %d log entries, peers %v", r.currentTerm, r.lastIndex(), r.cfg.Peers)
	r.mu.Unlock()

	r.wg.Add(2 + len(r.cfg.Peers))
	go r.ticker()
	go r.applier()
	for _, p := range r.cfg.Peers {
		go r.replicator(p)
	}
}

func (r *Raft) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	close(r.stopCh)
	r.applyCond.Broadcast()
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Raft) Status() Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Status{
		ID:           r.cfg.ID,
		Role:         r.role,
		Term:         r.currentTerm,
		LeaderID:     r.leaderID,
		CommitIndex:  r.commitIndex,
		LastApplied:  r.lastApplied,
		LastLogIndex: r.lastIndex(),
	}
}

func (r *Raft) Propose(cmd []byte) (index, term uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || r.role != Leader {
		return 0, 0, &NotLeaderError{LeaderID: r.leaderID}
	}
	r.appendLocked(Entry{Term: r.currentTerm, Command: cmd})
	r.advanceCommitLocked()
	r.triggerAll()
	return r.lastIndex(), r.currentTerm, nil
}

func (r *Raft) ticker() {
	defer r.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
		}
		r.mu.Lock()
		if r.role != Leader && time.Now().After(r.electionDeadline) {
			r.startElectionLocked()
		}
		r.mu.Unlock()
	}
}

func (r *Raft) startElectionLocked() {
	r.currentTerm++
	r.role = Candidate
	r.votedFor = r.cfg.ID
	r.leaderID = ""
	r.persistHardStateLocked()
	r.resetElectionTimerLocked()
	term := r.currentTerm
	r.logf("election timeout, campaigning for term %d", term)

	votes := 1
	if r.isMajority(votes) {
		r.becomeLeaderLocked()
		return
	}
	args := &RequestVoteArgs{Term: term, CandidateID: r.cfg.ID, LastLogIndex: r.lastIndex(), LastLogTerm: r.lastTerm()}
	for _, peer := range r.cfg.Peers {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
			defer cancel()
			reply, err := r.cfg.Transport.RequestVote(ctx, peer, args)
			if err != nil {
				return
			}
			r.mu.Lock()
			defer r.mu.Unlock()
			if r.stopped {
				return
			}
			if reply.Term > r.currentTerm {
				r.stepDownLocked(reply.Term, "")
				return
			}
			if r.role != Candidate || r.currentTerm != term || !reply.VoteGranted {
				return
			}
			votes++
			if r.isMajority(votes) {
				r.becomeLeaderLocked()
			}
		}()
	}
}

/** The no-op entry lets the new leader commit entries from earlier terms,
    which the current-term commit rule would otherwise leave hanging
**/
func (r *Raft) becomeLeaderLocked() {
	r.role = Leader
	r.leaderID = r.cfg.ID
	for _, p := range r.cfg.Peers {
		r.nextIndex[p] = r.lastIndex() + 1
		r.matchIndex[p] = 0
	}
	r.appendLocked(Entry{Term: r.currentTerm})
	r.logf("won election, leader for term %d (log up to %d, commit %d)", r.currentTerm, r.lastIndex(), r.commitIndex)
	r.advanceCommitLocked()
	r.triggerAll()
}

func (r *Raft) stepDownLocked(term uint64, leaderID string) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		r.persistHardStateLocked()
	}
	if r.role == Leader {
		r.resetElectionTimerLocked()
	}
	if r.role != Follower {
		r.logf("stepping down from %s to follower at term %d", r.role, r.currentTerm)
	}
	r.role = Follower
	r.leaderID = leaderID
}

func (r *Raft) replicator(peer string) {
	defer r.wg.Done()
	hb := time.NewTicker(r.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.triggers[peer]:
		case <-hb.C:
		}
		r.replicateTo(peer)
	}
}

func (r *Raft) replicateTo(peer string) {
	r.mu.Lock()
	if r.stopped || r.role != Leader {
		r.mu.Unlock()
		return
	}
	next := r.nextIndex[peer]
	prev := next - 1
	end := min(r.lastIndex(), prev+uint64(r.cfg.MaxBatch))
	args := &AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  r.log[prev].Term,
		Entries:      append([]Entry(nil), r.log[next:end+1]...),
		LeaderCommit: r.commitIndex,
	}
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), r.cfg.RPCTimeout)
	reply, err := r.cfg.Transport.AppendEntries(ctx, peer, args)
	cancel()
	if err != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return
	}
	if reply.Term > r.currentTerm {
		r.stepDownLocked(reply.Term, "")
		return
	}
	if r.role != Leader || r.currentTerm != args.Term {
		return
	}
	if reply.Success {
		match := args.PrevLogIndex + uint64(len(args.Entries))
		r.matchIndex[peer] = max(r.matchIndex[peer], match)
		r.nextIndex[peer] = match + 1
		r.advanceCommitLocked()
	} else {
		r.nextIndex[peer] = r.backupLocked(args, reply)
	}
	if r.nextIndex[peer] <= r.lastIndex() {
		r.trigger(peer)
	}
}

func (r *Raft) backupLocked(args *AppendEntriesArgs, reply *AppendEntriesReply) uint64 {
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		for i := args.PrevLogIndex; i > 0 && r.log[i].Term >= reply.ConflictTerm; i-- {
			if r.log[i].Term == reply.ConflictTerm {
				next = i + 1
				break
			}
		}
	}
	return max(1, min(next, args.PrevLogIndex))
}

// Only entries from the current term are committed by counting replicas (Raft paper, Figure 8)
func (r *Raft) advanceCommitLocked() {
	if r.role != Leader {
		return
	}
	for n := r.lastIndex(); n > r.commitIndex; n-- {
		if r.log[n].Term != r.currentTerm {
			return
		}
		count := 1
		for _, p := range r.cfg.Peers {
			if r.matchIndex[p] >= n {
				count++
			}
		}
		if r.isMajority(count) {
			r.commitIndex = n
			r.applyCond.Broadcast()
			return
		}
	}
}

func (r *Raft) applier() {
	defer r.wg.Done()
	r.mu.Lock()
	defer r.mu.Unlock()
	for {
		for !r.stopped && r.lastApplied >= r.commitIndex {
			r.applyCond.Wait()
		}
		if r.stopped {
			return
		}
		msgs := make([]ApplyMsg, 0, r.commitIndex-r.lastApplied)
		for i := r.lastApplied + 1; i <= r.commitIndex; i++ {
			msgs = append(msgs, ApplyMsg{Index: i, Term: r.log[i].Term, Command: r.log[i].Command})
		}
		r.mu.Unlock()
		for _, m := range msgs {
			r.apply(m)
		}
		r.mu.Lock()
		r.lastApplied = msgs[len(msgs)-1].Index
	}
}

func (r *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	if args.Term > r.currentTerm {
		r.stepDownLocked(args.Term, "")
	}
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return nil
	}
	upToDate := args.LastLogTerm > r.lastTerm() ||
		(args.LastLogTerm == r.lastTerm() && args.LastLogIndex >= r.lastIndex())
	if !upToDate || (r.votedFor != "" && r.votedFor != args.CandidateID) {
		return nil
	}
	if r.votedFor == "" {
		r.votedFor = args.CandidateID
		r.persistHardStateLocked()
		r.logf("voted for %s in term %d", args.CandidateID, r.currentTerm)
	}
	reply.VoteGranted = true
	r.resetElectionTimerLocked()
	return nil
}

func (r *Raft) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return ErrStopped
	}
	if args.Term > r.currentTerm {
		r.stepDownLocked(args.Term, "")
	}
	reply.Term = r.currentTerm
	if args.Term < r.currentTerm {
		return nil
	}
	if r.role == Leader {
		panic(fmt.Sprintf("raft: safety violation, two leaders (%s, %s) in term %d", r.cfg.ID, args.LeaderID, args.Term))
	}
	if r.role != Follower || r.leaderID != args.LeaderID {
		r.logf("following leader %s in term %d", args.LeaderID, args.Term)
		r.role = Follower
		r.leaderID = args.LeaderID
	}
	r.resetElectionTimerLocked()

	if args.PrevLogIndex > r.lastIndex() {
		reply.ConflictIndex = r.lastIndex() + 1
		return nil
	}
	if t := r.log[args.PrevLogIndex].Term; t != args.PrevLogTerm {
		reply.ConflictTerm = t
		i := args.PrevLogIndex
		for i > 1 && r.log[i-1].Term == t {
			i--
		}
		reply.ConflictIndex = i
		return nil
	}

	/** Only truncate on a real conflict: a delayed duplicate AppendEntries must
	    never delete entries this follower already acknowledged
	**/
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + uint64(i)
		if idx <= r.lastIndex() {
			if r.log[idx].Term == e.Term {
				continue
			}
			if idx <= r.commitIndex {
				panic(fmt.Sprintf("raft: safety violation, truncating committed index %d", idx))
			}
			r.logf("truncating conflicting log suffix from index %d", idx)
			r.log = r.log[:idx]
		}
		newEntries := args.Entries[i:]
		r.log = append(r.log, newEntries...)
		r.persistEntriesLocked(idx, newEntries)
		break
	}

	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if c := min(args.LeaderCommit, lastNew); c > r.commitIndex {
		r.commitIndex = c
		r.applyCond.Broadcast()
	}
	reply.Success = true
	return nil
}

func (r *Raft) appendLocked(e Entry) {
	r.log = append(r.log, e)
	r.persistEntriesLocked(r.lastIndex(), []Entry{e})
}

func (r *Raft) persistHardStateLocked() {
	if err := r.cfg.Storage.SaveHardState(HardState{Term: r.currentTerm, VotedFor: r.votedFor}); err != nil {
		panic(fmt.Sprintf("raft: persist hard state: %v", err))
	}
}

func (r *Raft) persistEntriesLocked(from uint64, entries []Entry) {
	if err := r.cfg.Storage.AppendEntries(from, entries); err != nil {
		panic(fmt.Sprintf("raft: persist entries from %d: %v", from, err))
	}
}

func (r *Raft) resetElectionTimerLocked() {
	spread := r.cfg.ElectionTimeoutMax - r.cfg.ElectionTimeoutMin
	r.electionDeadline = time.Now().Add(r.cfg.ElectionTimeoutMin + rand.N(spread))
}

func (r *Raft) trigger(peer string) {
	select {
	case r.triggers[peer] <- struct{}{}:
	default:
	}
}

func (r *Raft) triggerAll() {
	for _, p := range r.cfg.Peers {
		r.trigger(p)
	}
}

func (r *Raft) isMajority(n int) bool { return 2*n > len(r.cfg.Peers)+1 }

func (r *Raft) lastIndex() uint64 { return uint64(len(r.log) - 1) }

func (r *Raft) lastTerm() uint64 { return r.log[len(r.log)-1].Term }

func (r *Raft) logf(format string, args ...any) {
	r.cfg.Logger.Printf("raft: "+format, args...)
}
