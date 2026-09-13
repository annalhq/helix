package kv

import (
	"context"
	"errors"
	"time"

	"helix/internal/rpcpeer"
)

// The leader gets a slightly shorter deadline so it can answer before the follower gives up
const forwardMargin = 100 * time.Millisecond

type ForwardArgs struct {
	Cmd     Command
	Timeout time.Duration
}

type ForwardReply struct {
	Result Result
	Err    *Error
}

type ForwardServer struct {
	svc *Service
}

func NewForwardServer(svc *Service) *ForwardServer {
	return &ForwardServer{svc: svc}
}

func (f *ForwardServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	ctx, cancel := context.WithTimeout(context.Background(), min(args.Timeout, RequestTimeout))
	defer cancel()
	res, err := f.svc.submit(ctx, args.Cmd, false)
	if err != nil {
		reply.Err = AsError(err)
		return nil
	}
	reply.Result = res
	return nil
}

type RPCForwarder struct {
	clients *rpcpeer.Clients
}

func NewRPCForwarder(clients *rpcpeer.Clients) *RPCForwarder {
	return &RPCForwarder{clients: clients}
}

func (f *RPCForwarder) Forward(ctx context.Context, leaderID string, cmd Command) (Result, error) {
	timeout := RequestTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline) - forwardMargin
	}
	if timeout <= 0 {
		return Result{}, Unavailable("no time left to forward to " + leaderID)
	}

	var reply ForwardReply
	err := f.clients.Call(ctx, leaderID, "KV.Forward", &ForwardArgs{Cmd: cmd, Timeout: timeout}, &reply)
	if errors.Is(err, rpcpeer.ErrNotSent) {
		return Result{}, Unavailable(err.Error())
	}
	if err != nil {
		return Result{}, err
	}
	if reply.Err != nil {
		return Result{}, reply.Err
	}
	return reply.Result, nil
}
