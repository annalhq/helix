package raft

import (
	"context"

	"helix/internal/rpcpeer"
)

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  string
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderID     string
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}

type Transport interface {
	RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

type RPCTransport struct {
	clients *rpcpeer.Clients
}

func NewRPCTransport(clients *rpcpeer.Clients) *RPCTransport {
	return &RPCTransport{clients: clients}
}

func (t *RPCTransport) RequestVote(ctx context.Context, peer string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	return reply, t.clients.Call(ctx, peer, "Raft.RequestVote", args, reply)
}

func (t *RPCTransport) AppendEntries(ctx context.Context, peer string, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	return reply, t.clients.Call(ctx, peer, "Raft.AppendEntries", args, reply)
}

type RPCServer struct {
	r *Raft
}

func NewRPCServer(r *Raft) *RPCServer {
	return &RPCServer{r: r}
}

func (s *RPCServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return s.r.HandleRequestVote(args, reply)
}

func (s *RPCServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return s.r.HandleAppendEntries(args, reply)
}
