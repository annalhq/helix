package kv

import (
	"context"
	"testing"
)

func TestParseReadMode(t *testing.T) {
	for _, s := range []string{"local", "log"} {
		if m, err := ParseReadMode(s); err != nil || string(m) != s {
			t.Errorf("ParseReadMode(%q) = %q, %v", s, m, err)
		}
	}
	if _, err := ParseReadMode("quorum"); err == nil {
		t.Error("ParseReadMode(quorum) should fail")
	}
}

func TestServiceRejectsInvalidCommands(t *testing.T) {
	svc := NewService(NewStateMachine(), ReadLocal)
	for _, cmd := range []Command{{Op: "del", Key: "k0"}, {Op: OpGet}} {
		_, err := svc.Submit(context.Background(), cmd)
		if err == nil {
			t.Fatalf("Submit(%+v) should fail", cmd)
		}
		if e := AsError(err); e.Code != CodeBadRequest || e.Outcome != Definite {
			t.Errorf("Submit(%+v) = %v (%s), want bad_request/definite", cmd, e, e.Outcome)
		}
	}
}
