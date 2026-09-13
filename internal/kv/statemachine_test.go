package kv

import "testing"

func intp(v int) *int { return &v }

func TestStateMachineApply(t *testing.T) {
	sm := NewStateMachine()
	steps := []struct {
		cmd   Command
		ok    bool
		value *int
	}{
		{Command{Op: OpGet, Key: "k0"}, true, nil},
		{Command{Op: OpCAS, Key: "k0", Old: 0, New: 1}, false, nil},
		{Command{Op: OpPut, Key: "k0", Value: 3}, true, nil},
		{Command{Op: OpGet, Key: "k0"}, true, intp(3)},
		{Command{Op: OpCAS, Key: "k0", Old: 2, New: 4}, false, nil},
		{Command{Op: OpGet, Key: "k0"}, true, intp(3)},
		{Command{Op: OpCAS, Key: "k0", Old: 3, New: 4}, true, nil},
		{Command{Op: OpGet, Key: "k0"}, true, intp(4)},
		{Command{Op: OpGet, Key: "k1"}, true, nil},
	}
	for i, s := range steps {
		got := sm.Apply(s.cmd)
		if got.OK != s.ok || !equalIntPtr(got.Value, s.value) {
			t.Fatalf("step %d %+v: got ok=%v value=%v, want ok=%v value=%v",
				i, s.cmd, got.OK, deref(got.Value), s.ok, deref(s.value))
		}
	}
}

func equalIntPtr(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func deref(p *int) any {
	if p == nil {
		return nil
	}
	return *p
}
