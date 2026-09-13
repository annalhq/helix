package rpcpeer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

/** ErrNotSent means the request never left this process, so it definitely had
    no effect. Every other error after a call starts is indeterminate
**/
var ErrNotSent = errors.New("rpc not sent")

type Clients struct {
	addrs map[string]string

	mu    sync.Mutex
	conns map[string]*rpc.Client
}

func New(addrs map[string]string) *Clients {
	return &Clients{addrs: addrs, conns: make(map[string]*rpc.Client)}
}

/** On timeout the connection is dropped rather than reused: after a partition
    heals, a fresh dial recovers immediately, while the old TCP connection may
    sit in retransmission backoff for seconds
**/
func (c *Clients) Call(ctx context.Context, peer, method string, args, reply any) error {
	cl, err := c.client(ctx, peer)
	if err != nil {
		return fmt.Errorf("%w: %s: %v", ErrNotSent, peer, err)
	}
	call := cl.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			var serverErr rpc.ServerError
			if !errors.As(call.Error, &serverErr) {
				c.drop(peer, cl)
			}
		}
		return call.Error
	case <-ctx.Done():
		c.drop(peer, cl)
		return ctx.Err()
	}
}

func (c *Clients) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for peer, cl := range c.conns {
		cl.Close()
		delete(c.conns, peer)
	}
}

func (c *Clients) client(ctx context.Context, peer string) (*rpc.Client, error) {
	c.mu.Lock()
	if cl := c.conns[peer]; cl != nil {
		c.mu.Unlock()
		return cl, nil
	}
	addr, ok := c.addrs[peer]
	c.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	cl := rpc.NewClient(conn)

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.conns[peer]; existing != nil {
		cl.Close()
		return existing, nil
	}
	c.conns[peer] = cl
	return cl, nil
}

func (c *Clients) drop(peer string, cl *rpc.Client) {
	c.mu.Lock()
	if c.conns[peer] == cl {
		delete(c.conns, peer)
	}
	c.mu.Unlock()
	cl.Close()
}

func Serve(ln net.Listener, srv *rpc.Server) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go srv.ServeConn(conn)
	}
}
