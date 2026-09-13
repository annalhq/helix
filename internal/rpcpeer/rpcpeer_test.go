package rpcpeer

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"testing"
	"time"
)

type Echo struct{}

type EchoArgs struct {
	Msg   string
	Delay time.Duration
}

type EchoReply struct {
	Msg string
}

func (Echo) Echo(args *EchoArgs, reply *EchoReply) error {
	time.Sleep(args.Delay)
	reply.Msg = args.Msg
	return nil
}

func startEchoServer(t *testing.T) string {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("Echo", Echo{}); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go Serve(ln, srv)
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

func call(c *Clients, peer string, timeout time.Duration, args *EchoArgs) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var reply EchoReply
	err := c.Call(ctx, peer, "Echo.Echo", args, &reply)
	return reply.Msg, err
}

func TestCallRoundTrip(t *testing.T) {
	c := New(map[string]string{"a": startEchoServer(t)})
	defer c.Close()
	for _, msg := range []string{"one", "two"} {
		got, err := call(c, "a", time.Second, &EchoArgs{Msg: msg})
		if err != nil || got != msg {
			t.Fatalf("Echo(%q) = %q, %v", msg, got, err)
		}
	}
}

func TestCallUnreachablePeerIsNotSent(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	c := New(map[string]string{"down": addr})
	defer c.Close()
	for _, peer := range []string{"down", "unknown"} {
		if _, err := call(c, peer, time.Second, &EchoArgs{Msg: "x"}); !errors.Is(err, ErrNotSent) {
			t.Errorf("call to %s: err = %v, want ErrNotSent", peer, err)
		}
	}
}

func TestCallTimeoutIsIndeterminateAndReconnects(t *testing.T) {
	c := New(map[string]string{"a": startEchoServer(t)})
	defer c.Close()

	_, err := call(c, "a", 50*time.Millisecond, &EchoArgs{Msg: "slow", Delay: 500 * time.Millisecond})
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrNotSent) {
		t.Fatalf("slow call: err = %v, want deadline exceeded (not ErrNotSent)", err)
	}
	if got, err := call(c, "a", time.Second, &EchoArgs{Msg: "fast"}); err != nil || got != "fast" {
		t.Fatalf("call after timeout = %q, %v", got, err)
	}
}
