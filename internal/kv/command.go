package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Op string

const (
	OpGet Op = "get"
	OpPut Op = "put"
	OpCAS Op = "cas"
)

type Command struct {
	Op    Op
	Key   string
	Value int
	Old   int
	New   int
}

type Result struct {
	OK    bool
	Value *int
}

func (c Command) Validate() error {
	switch c.Op {
	case OpGet, OpPut, OpCAS:
	default:
		return BadRequest(fmt.Sprintf("unknown op %q", c.Op))
	}
	if c.Key == "" {
		return BadRequest("missing key")
	}
	return nil
}

func encodeCommand(cmd Command) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		panic(fmt.Sprintf("kv: encode command: %v", err))
	}
	return buf.Bytes()
}

func decodeCommand(b []byte) (Command, error) {
	var cmd Command
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&cmd)
	return cmd, err
}
