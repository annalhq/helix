package kv

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"helix/internal/raft"
)

type ReadMode string

const (
	ReadLocal ReadMode = "local"
	ReadLog   ReadMode = "log"
)

func ParseReadMode(s string) (ReadMode, error) {
	switch m := ReadMode(s); m {
	case ReadLocal, ReadLog:
		return m, nil
	}
	return "", fmt.Errorf("invalid read mode %q (want local or log)", s)
}

type Consensus interface {
	Propose(cmd []byte) (index, term uint64, err error)
	Status() raft.Status
}

type Forwarder interface {
	Forward(ctx context.Context, leaderID string, cmd Command) (Result, error)
}

type NodeStatus struct {
	ID           string   `json:"id"`
	Role         string   `json:"role"`
	Term         uint64   `json:"term"`
	Leader       string   `json:"leader"`
	CommitIndex  uint64   `json:"commit_index"`
	LastApplied  uint64   `json:"last_applied"`
	LastLogIndex uint64   `json:"last_log_index"`
	ReadMode     ReadMode `json:"read_mode"`
}

type applied struct {
	result Result
	err    error
}

type waiter struct {
	term uint64
	ch   chan applied
}

type Service struct {
	sm        *StateMachine
	readMode  ReadMode
	consensus Consensus
	forwarder Forwarder

	mu      sync.Mutex
	waiters map[uint64]waiter
}

func NewService(sm *StateMachine, readMode ReadMode) *Service {
	return &Service{sm: sm, readMode: readMode, waiters: make(map[uint64]waiter)}
}

func (s *Service) Attach(c Consensus, f Forwarder) {
	s.consensus = c
	s.forwarder = f
}

func (s *Service) ReadMode() ReadMode { return s.readMode }

func (s *Service) Status() NodeStatus {
	st := s.consensus.Status()
	return NodeStatus{
		ID:           st.ID,
		Role:         string(st.Role),
		Term:         st.Term,
		Leader:       st.LeaderID,
		CommitIndex:  st.CommitIndex,
		LastApplied:  st.LastApplied,
		LastLogIndex: st.LastLogIndex,
		ReadMode:     s.readMode,
	}
}

func (s *Service) Submit(ctx context.Context, cmd Command) (Result, error) {
	return s.submit(ctx, cmd, true)
}

func (s *Service) submit(ctx context.Context, cmd Command, mayForward bool) (Result, error) {
	if err := cmd.Validate(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	// read-mode=local: any node answers from whatever it has applied, even a deposed leader
	if cmd.Op == OpGet && s.readMode == ReadLocal {
		return s.sm.Apply(cmd), nil
	}
	return s.replicate(ctx, cmd, mayForward)
}

/** s.mu is held across Propose and waiter registration so Apply cannot
    deliver the entry before anyone is waiting for it
**/
func (s *Service) replicate(ctx context.Context, cmd Command, mayForward bool) (Result, error) {
	data := encodeCommand(cmd)
	s.mu.Lock()
	index, term, err := s.consensus.Propose(data)
	if err != nil {
		s.mu.Unlock()
		var notLeader *raft.NotLeaderError
		if !errors.As(err, &notLeader) {
			return Result{}, err
		}
		if !mayForward {
			return Result{}, NotLeader(notLeader.LeaderID)
		}
		if notLeader.LeaderID == "" || s.forwarder == nil {
			return Result{}, Unavailable("no known leader")
		}
		return s.forwarder.Forward(ctx, notLeader.LeaderID, cmd)
	}
	ch := make(chan applied, 1)
	s.waiters[index] = waiter{term: term, ch: ch}
	s.mu.Unlock()

	select {
	case a := <-ch:
		return a.result, a.err
	case <-ctx.Done():
		s.mu.Lock()
		if w, ok := s.waiters[index]; ok && w.ch == ch {
			delete(s.waiters, index)
		}
		s.mu.Unlock()
		select {
		case a := <-ch:
			return a.result, a.err
		default:
			return Result{}, ctx.Err()
		}
	}
}

// A different term at our index means another leader's entry won the slot, so ours was never applied there
func (s *Service) Apply(msg raft.ApplyMsg) {
	var res Result
	if len(msg.Command) > 0 {
		cmd, err := decodeCommand(msg.Command)
		if err != nil {
			panic(fmt.Sprintf("kv: undecodable log entry %d: %v", msg.Index, err))
		}
		res = s.sm.Apply(cmd)
	}

	s.mu.Lock()
	w, ok := s.waiters[msg.Index]
	delete(s.waiters, msg.Index)
	s.mu.Unlock()
	if !ok {
		return
	}
	if w.term == msg.Term {
		w.ch <- applied{result: res}
	} else {
		w.ch <- applied{err: LeadershipLost()}
	}
}
