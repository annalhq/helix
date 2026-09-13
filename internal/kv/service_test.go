package kv

import (
	"context"
	"testing"
	"time"

	"helix/internal/raft"
)

func newSingleNodeService(t *testing.T, mode ReadMode) (*Service, *raft.Raft) {
	t.Helper()
	svc := NewService(NewStateMachine(), mode)
	r, err := raft.New(raft.Config{
		ID:                 "solo",
		Storage:            raft.NewMemoryStorage(),
		ElectionTimeoutMin: 20 * time.Millisecond,
		ElectionTimeoutMax: 40 * time.Millisecond,
	}, svc.Apply)
	if err != nil {
		t.Fatal(err)
	}
	svc.Attach(r, nil)
	r.Start()
	t.Cleanup(r.Stop)

	deadline := time.Now().Add(2 * time.Second)
	for r.Status().Role != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatal("single node never became leader")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return svc, r
}

func submit(t *testing.T, svc *Service, cmd Command) (Result, *Error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := svc.Submit(ctx, cmd)
	if err != nil {
		return res, AsError(err)
	}
	return res, nil
}

func expectCode(t *testing.T, what string, got *Error, code string, outcome Outcome) {
	t.Helper()
	if got == nil || got.Code != code || got.Outcome != outcome {
		t.Fatalf("%s: error %v, want %s/%s", what, got, code, outcome)
	}
}

func TestParseReadMode(t *testing.T) {
	for _, s := range []string{"leader", "log"} {
		if m, err := ParseReadMode(s); err != nil || string(m) != s {
			t.Errorf("ParseReadMode(%q) = %q, %v", s, m, err)
		}
	}
	for _, s := range []string{"local", "quorum"} {
		if _, err := ParseReadMode(s); err == nil {
			t.Errorf("ParseReadMode(%q) should fail", s)
		}
	}
}

func TestServiceRejectsInvalidCommands(t *testing.T) {
	svc := NewService(NewStateMachine(), ReadLeader)
	for _, cmd := range []Command{{Op: "del", Key: "k0"}, {Op: OpGet}} {
		_, err := submit(t, svc, cmd)
		expectCode(t, "invalid command", err, CodeBadRequest, Definite)
	}
}

func TestServiceReplicatesEverythingInLogMode(t *testing.T) {
	svc, r := newSingleNodeService(t, ReadLog)
	start := r.Status().LastLogIndex

	for _, cmd := range []Command{
		{Op: OpPut, Key: "k0", Value: 1},
		{Op: OpCAS, Key: "k0", Old: 1, New: 2},
	} {
		if res, err := submit(t, svc, cmd); err != nil || !res.OK {
			t.Fatalf("%+v: %+v %v", cmd, res, err)
		}
	}
	res, err := submit(t, svc, Command{Op: OpGet, Key: "k0"})
	if err != nil || res.Value == nil || *res.Value != 2 {
		t.Fatalf("get: %+v %v", res, err)
	}
	if got := r.Status().LastLogIndex - start; got != 3 {
		t.Fatalf("log grew by %d entries, want 3 (reads go through the log)", got)
	}
}

func TestServiceLeaderReadsBypassTheLog(t *testing.T) {
	svc, r := newSingleNodeService(t, ReadLeader)
	if _, err := submit(t, svc, Command{Op: OpPut, Key: "k0", Value: 7}); err != nil {
		t.Fatal(err)
	}
	before := r.Status().LastLogIndex
	res, err := submit(t, svc, Command{Op: OpGet, Key: "k0"})
	if err != nil || res.Value == nil || *res.Value != 7 {
		t.Fatalf("get: %+v %v", res, err)
	}
	if after := r.Status().LastLogIndex; after != before {
		t.Fatalf("leader read appended to the log (%d -> %d)", before, after)
	}
}

type fakeConsensus struct {
	err      error
	term     uint64
	index    uint64
	proposed chan raft.ApplyMsg
	status   raft.Status
}

func (f *fakeConsensus) Propose(cmd []byte) (uint64, uint64, error) {
	if f.err != nil {
		return 0, 0, f.err
	}
	f.index++
	f.proposed <- raft.ApplyMsg{Index: f.index, Term: f.term, Command: cmd}
	return f.index, f.term, nil
}

func (f *fakeConsensus) Status() raft.Status { return f.status }

type fakeForwarder func(ctx context.Context, leaderID string, cmd Command) (Result, error)

func (f fakeForwarder) Forward(ctx context.Context, leaderID string, cmd Command) (Result, error) {
	return f(ctx, leaderID, cmd)
}

func TestServiceWhenNotLeader(t *testing.T) {
	put := Command{Op: OpPut, Key: "k0", Value: 1}

	svc := NewService(NewStateMachine(), ReadLog)
	svc.Attach(&fakeConsensus{err: &raft.NotLeaderError{}}, nil)
	_, err := submit(t, svc, put)
	expectCode(t, "no known leader", err, CodeUnavailable, Definite)

	var forwardedTo string
	svc.Attach(&fakeConsensus{err: &raft.NotLeaderError{LeaderID: "kv-1"}}, fakeForwarder(
		func(_ context.Context, leaderID string, _ Command) (Result, error) {
			forwardedTo = leaderID
			return Result{OK: true}, nil
		}))
	if res, err := submit(t, svc, put); err != nil || !res.OK || forwardedTo != "kv-1" {
		t.Fatalf("forward: %+v %v, forwarded to %q", res, err, forwardedTo)
	}

	_, fwdErr := svc.submit(context.Background(), put, false)
	expectCode(t, "forwarded request on a non-leader", AsError(fwdErr), CodeNotLeader, Definite)
	if AsError(fwdErr).LeaderHint != "kv-1" {
		t.Fatalf("leader hint %q, want kv-1", AsError(fwdErr).LeaderHint)
	}
}

func TestServiceLeaderReadOnFollower(t *testing.T) {
	get := Command{Op: OpGet, Key: "k0"}

	svc := NewService(NewStateMachine(), ReadLeader)
	svc.Attach(&fakeConsensus{status: raft.Status{Role: raft.Follower}}, nil)
	_, err := submit(t, svc, get)
	expectCode(t, "follower with no known leader", err, CodeUnavailable, Definite)

	var forwardedTo string
	var forwarded Command
	svc.Attach(&fakeConsensus{status: raft.Status{Role: raft.Follower, LeaderID: "kv-2"}}, fakeForwarder(
		func(_ context.Context, leaderID string, cmd Command) (Result, error) {
			forwardedTo, forwarded = leaderID, cmd
			v := 5
			return Result{OK: true, Value: &v}, nil
		}))
	res, err := submit(t, svc, get)
	if err != nil || res.Value == nil || *res.Value != 5 || forwardedTo != "kv-2" || forwarded != get {
		t.Fatalf("forwarded read: %+v %v, sent %+v to %q", res, err, forwarded, forwardedTo)
	}

	_, fwdErr := svc.submit(context.Background(), get, false)
	expectCode(t, "forwarded read on a non-leader", AsError(fwdErr), CodeNotLeader, Definite)
}

func TestServiceWaitsForItsOwnEntry(t *testing.T) {
	fc := &fakeConsensus{term: 5, proposed: make(chan raft.ApplyMsg, 1)}
	svc := NewService(NewStateMachine(), ReadLog)
	svc.Attach(fc, nil)

	type outcome struct {
		res Result
		err *Error
	}
	run := func(timeout time.Duration) <-chan outcome {
		done := make(chan outcome, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			res, err := svc.Submit(ctx, Command{Op: OpPut, Key: "k0", Value: 1})
			if err != nil {
				done <- outcome{res, AsError(err)}
				return
			}
			done <- outcome{res, nil}
		}()
		return done
	}

	done := run(time.Second)
	svc.Apply(<-fc.proposed)
	if o := <-done; o.err != nil || !o.res.OK {
		t.Fatalf("same term apply: %+v %v", o.res, o.err)
	}

	done = run(time.Second)
	msg := <-fc.proposed
	msg.Term, msg.Command = 6, nil
	svc.Apply(msg)
	expectCode(t, "entry replaced by another leader", (<-done).err, CodeLeadershipLost, Unknown)

	done = run(30 * time.Millisecond)
	<-fc.proposed
	expectCode(t, "never committed", (<-done).err, CodeTimeout, Unknown)
	svc.mu.Lock()
	defer svc.mu.Unlock()
	if len(svc.waiters) != 0 {
		t.Fatalf("%d waiters leaked", len(svc.waiters))
	}
}

func TestServiceStatus(t *testing.T) {
	svc, _ := newSingleNodeService(t, ReadLog)
	st := svc.Status()
	if st.ID != "solo" || st.Role != "leader" || st.Leader != "solo" || st.ReadMode != ReadLog || st.Term == 0 {
		t.Fatalf("status %+v", st)
	}
}
