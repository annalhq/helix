package kv

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"testing"
	"time"
)

type obj = map[string]any

type submitFunc func(ctx context.Context, cmd Command) (Result, error)

func (f submitFunc) Submit(ctx context.Context, cmd Command) (Result, error) { return f(ctx, cmd) }

func (f submitFunc) Status() NodeStatus {
	return NodeStatus{ID: "kv-1", Role: "leader", Term: 4, Leader: "kv-1", CommitIndex: 9, LastApplied: 9, LastLogIndex: 10, ReadMode: ReadLog}
}

type edgeConn struct {
	t    *testing.T
	conn net.Conn
	r    *bufio.Reader
}

func discardLogger() *log.Logger { return log.New(io.Discard, "", 0) }

func dialEdge(t *testing.T, e *Edge) *edgeConn {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go e.Serve(ln)
	t.Cleanup(func() { ln.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return &edgeConn{t: t, conn: conn, r: bufio.NewReader(conn)}
}

func dialRealEdge(t *testing.T) *edgeConn {
	t.Helper()
	svc, _ := newSingleNodeService(t, ReadLeader)
	return dialEdge(t, NewEdge(svc, discardLogger()))
}

func (c *edgeConn) send(line string) obj {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, line+"\n"); err != nil {
		c.t.Fatal(err)
	}
	raw, err := c.r.ReadBytes('\n')
	if err != nil {
		c.t.Fatal(err)
	}
	var m obj
	if err := json.Unmarshal(raw, &m); err != nil {
		c.t.Fatalf("bad response %q: %v", raw, err)
	}
	return m
}

func expect(t *testing.T, req string, got, want obj) {
	t.Helper()
	for k, w := range want {
		if g, ok := got[k]; !ok || g != w {
			t.Errorf("%s: %s = %v (present=%v), want %v; response %v", req, k, g, ok, w, got)
		}
	}
}

func TestEdgeRoundTrip(t *testing.T) {
	c := dialRealEdge(t)
	steps := []struct {
		req  string
		want obj
	}{
		{`{"id":1,"op":"put","key":"k0","value":3}`, obj{"id": 1.0, "ok": true, "value": nil, "outcome": "applied"}},
		{`{"id":"two","op":"get","key":"k0"}`, obj{"id": "two", "ok": true, "value": 3.0, "outcome": "applied"}},
		{`{"id":3,"op":"cas","key":"k0","old":1,"new":2}`, obj{"ok": false, "err": "cas_mismatch", "outcome": "applied"}},
		{`{"id":4,"op":"cas","key":"k0","old":3,"new":4}`, obj{"ok": true, "outcome": "applied"}},
		{`{"id":5,"op":"get","key":"k0"}`, obj{"ok": true, "value": 4.0}},
		{`{"id":6,"op":"get","key":"k9"}`, obj{"ok": true, "value": nil, "outcome": "applied"}},
		{`{"id":7,"op":"cas","key":"k9","old":0,"new":1}`, obj{"ok": false, "err": "cas_mismatch"}},
	}
	for _, s := range steps {
		expect(t, s.req, c.send(s.req), s.want)
	}
}

func TestEdgeBadRequests(t *testing.T) {
	c := dialRealEdge(t)
	bad := []struct {
		req string
		id  any
	}{
		{`not json`, nil},
		{`{"id":1,"op":"put","key":"k0"}`, 1.0},
		{`{"id":2,"op":"cas","key":"k0","old":1}`, 2.0},
		{`{"id":3,"op":"del","key":"k0"}`, 3.0},
		{`{"id":4,"op":"get"}`, 4.0},
		{`{"id":5,"op":"put","key":"k0","value":"3"}`, nil},
		{`{"id":6,"op":"put","key":"k0","value":1.5}`, nil},
	}
	for _, b := range bad {
		expect(t, b.req, c.send(b.req), obj{"id": b.id, "ok": false, "err": "bad_request", "outcome": "definite"})
	}
	expect(t, "after bad requests", c.send(`{"id":9,"op":"get","key":"k0"}`), obj{"id": 9.0, "outcome": "applied"})
}

func TestEdgeStatus(t *testing.T) {
	c := dialEdge(t, NewEdge(submitFunc(nil), discardLogger()))
	got := c.send(`{"id":1,"op":"status"}`)
	expect(t, "status", got, obj{"id": 1.0, "ok": true, "outcome": "applied"})
	st, _ := got["status"].(obj)
	expect(t, "status body", st, obj{"id": "kv-1", "role": "leader", "term": 4.0, "leader": "kv-1", "last_log_index": 10.0, "read_mode": "log"})
}

func TestEdgeErrorOutcomes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want obj
	}{
		{"not leader", NotLeader("kv-1"), obj{"ok": false, "err": "not_leader", "outcome": "definite", "leader_hint": "kv-1"}},
		{"unavailable", Unavailable("no leader"), obj{"err": "unavailable", "outcome": "definite"}},
		{"leadership lost", LeadershipLost(), obj{"err": "leadership_lost", "outcome": "unknown"}},
		{"deadline", context.DeadlineExceeded, obj{"err": "timeout", "outcome": "unknown"}},
		{"unexpected", errors.New("boom"), obj{"err": "internal", "outcome": "unknown", "detail": "boom"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := submitFunc(func(context.Context, Command) (Result, error) { return Result{}, tc.err })
			c := dialEdge(t, NewEdge(svc, discardLogger()))
			req := `{"id":1,"op":"put","key":"k0","value":1}`
			expect(t, req, c.send(req), tc.want)
		})
	}
}

func TestEdgeRequestTimeout(t *testing.T) {
	svc := submitFunc(func(ctx context.Context, _ Command) (Result, error) {
		<-ctx.Done()
		return Result{}, ctx.Err()
	})
	e := NewEdge(svc, discardLogger())
	e.timeout = 20 * time.Millisecond
	c := dialEdge(t, e)
	req := `{"id":1,"op":"cas","key":"k0","old":1,"new":2}`
	expect(t, req, c.send(req), obj{"ok": false, "err": "timeout", "outcome": "unknown"})
}
