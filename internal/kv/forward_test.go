package kv

import (
	"context"
	"net"
	"net/rpc"
	"testing"
	"time"

	"helix/internal/raft"
	"helix/internal/rpcpeer"
)

func serveForwarding(t *testing.T, svc *Service) string {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("KV", NewForwardServer(svc)); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go rpcpeer.Serve(ln, srv)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func followerOf(t *testing.T, leaderID, leaderAddr string) *Service {
	t.Helper()
	clients := rpcpeer.New(map[string]string{leaderID: leaderAddr})
	t.Cleanup(clients.Close)
	svc := NewService(NewStateMachine(), ReadLog)
	svc.Attach(&fakeConsensus{err: &raft.NotLeaderError{LeaderID: leaderID}}, NewRPCForwarder(clients))
	return svc
}

func TestForwardOverRPC(t *testing.T) {
	leader, _ := newSingleNodeService(t, ReadLog)
	follower := followerOf(t, "solo", serveForwarding(t, leader))

	if res, err := submit(t, follower, Command{Op: OpPut, Key: "k0", Value: 3}); err != nil || !res.OK {
		t.Fatalf("forwarded put: %+v %v", res, err)
	}
	if res, err := submit(t, follower, Command{Op: OpCAS, Key: "k0", Old: 1, New: 2}); err != nil || res.OK {
		t.Fatalf("forwarded mismatching cas: %+v %v", res, err)
	}
	res, err := submit(t, follower, Command{Op: OpGet, Key: "k0"})
	if err != nil || res.Value == nil || *res.Value != 3 {
		t.Fatalf("forwarded get: %+v %v", res, err)
	}
}

func TestForwardToUnreachableLeaderIsDefinite(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	_, fwdErr := submit(t, followerOf(t, "gone", addr), Command{Op: OpPut, Key: "k0", Value: 1})
	expectCode(t, "leader unreachable", fwdErr, CodeUnavailable, Definite)
}

func TestForwardIsNotReforwarded(t *testing.T) {
	stale := NewService(NewStateMachine(), ReadLog)
	stale.Attach(&fakeConsensus{err: &raft.NotLeaderError{LeaderID: "newer"}}, fakeForwarder(
		func(context.Context, string, Command) (Result, error) {
			t.Error("a forwarded request must not be forwarded again")
			return Result{}, nil
		}))
	follower := followerOf(t, "stale", serveForwarding(t, stale))

	_, err := submit(t, follower, Command{Op: OpPut, Key: "k0", Value: 1})
	expectCode(t, "forward to a node that lost leadership", err, CodeNotLeader, Definite)
	if err.LeaderHint != "newer" {
		t.Fatalf("leader hint %q, want newer", err.LeaderHint)
	}
}

func TestForwardHonoursCallerDeadline(t *testing.T) {
	leaderSvc := NewService(NewStateMachine(), ReadLog)
	leaderSvc.Attach(&fakeConsensus{term: 1, proposed: make(chan raft.ApplyMsg, 8)}, nil)
	follower := followerOf(t, "slow", serveForwarding(t, leaderSvc))

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := follower.Submit(ctx, Command{Op: OpPut, Key: "k0", Value: 1})
	expectCode(t, "leader never commits", AsError(err), CodeTimeout, Unknown)
	if elapsed := time.Since(start); elapsed > 350*time.Millisecond {
		t.Fatalf("forward took %v, longer than the caller's deadline", elapsed)
	}
}
