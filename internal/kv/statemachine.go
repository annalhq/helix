package kv

import (
	"fmt"
	"sync"
)

type StateMachine struct {
	mu   sync.Mutex
	data map[string]int
}

func NewStateMachine() *StateMachine {
	return &StateMachine{data: make(map[string]int)}
}

// A missing key holds no value, so CAS against it never matches, not even old=0
func (s *StateMachine) Apply(cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, exists := s.data[cmd.Key]
	switch cmd.Op {
	case OpGet:
		if !exists {
			return Result{OK: true}
		}
		return Result{OK: true, Value: &cur}
	case OpPut:
		s.data[cmd.Key] = cmd.Value
		return Result{OK: true}
	case OpCAS:
		if !exists || cur != cmd.Old {
			return Result{OK: false}
		}
		s.data[cmd.Key] = cmd.New
		return Result{OK: true}
	}
	panic(fmt.Sprintf("kv: apply of invalid op %q", cmd.Op))
}
