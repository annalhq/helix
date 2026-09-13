package kv

import "fmt"

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
