package kv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"time"
)

const (
	RequestTimeout = 1500 * time.Millisecond
	maxLineBytes   = 64 << 10
)

type Submitter interface {
	Submit(ctx context.Context, cmd Command) (Result, error)
}

type request struct {
	ID    json.RawMessage `json:"id"`
	Op    string          `json:"op"`
	Key   string          `json:"key"`
	Value *int            `json:"value"`
	Old   *int            `json:"old"`
	New   *int            `json:"new"`
}

type response struct {
	ID         json.RawMessage `json:"id"`
	OK         bool            `json:"ok"`
	Value      *int            `json:"value"`
	Err        string          `json:"err,omitempty"`
	Detail     string          `json:"detail,omitempty"`
	Outcome    Outcome         `json:"outcome"`
	LeaderHint string          `json:"leader_hint,omitempty"`
}

type Edge struct {
	svc     Submitter
	logger  *log.Logger
	timeout time.Duration
}

func NewEdge(svc Submitter, logger *log.Logger) *Edge {
	return &Edge{svc: svc, logger: logger, timeout: RequestTimeout}
}

func (e *Edge) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go e.handle(conn)
	}
}

func (e *Edge) handle(conn net.Conn) {
	defer conn.Close()
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 4096), maxLineBytes)
	w := bufio.NewWriter(conn)
	enc := json.NewEncoder(w)

	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if err := enc.Encode(e.process(line)); err != nil {
			return
		}
		if err := w.Flush(); err != nil {
			return
		}
	}
	if err := sc.Err(); err != nil {
		e.logger.Printf("edge: %s: %v", conn.RemoteAddr(), err)
	}
}

func (e *Edge) process(line []byte) response {
	var req request
	if err := json.Unmarshal(line, &req); err != nil {
		return errorResponse(nil, BadRequest("invalid json: "+err.Error()))
	}
	cmd, err := req.command()
	if err != nil {
		return errorResponse(req.ID, AsError(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	res, err := e.svc.Submit(ctx, cmd)
	if err != nil {
		return errorResponse(req.ID, AsError(err))
	}

	resp := response{ID: req.ID, OK: res.OK, Value: res.Value, Outcome: Applied}
	if cmd.Op == OpCAS && !res.OK {
		resp.Err = CodeCASMismatch
	}
	return resp
}

func (r request) command() (Command, error) {
	cmd := Command{Op: Op(r.Op), Key: r.Key}
	switch cmd.Op {
	case OpPut:
		if r.Value == nil {
			return cmd, BadRequest("put requires value")
		}
		cmd.Value = *r.Value
	case OpCAS:
		if r.Old == nil || r.New == nil {
			return cmd, BadRequest("cas requires old and new")
		}
		cmd.Old, cmd.New = *r.Old, *r.New
	}
	return cmd, nil
}

func errorResponse(id json.RawMessage, e *Error) response {
	return response{ID: id, Err: e.Code, Detail: e.Detail, Outcome: e.Outcome, LeaderHint: e.LeaderHint}
}
