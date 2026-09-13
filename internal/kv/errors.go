package kv

import (
	"context"
	"errors"
)

type Outcome string

const (
	Applied  Outcome = "applied"
	Definite Outcome = "definite"
	Unknown  Outcome = "unknown"
)

const (
	CodeBadRequest     = "bad_request"
	CodeCASMismatch    = "cas_mismatch"
	CodeUnavailable    = "unavailable"
	CodeNotLeader      = "not_leader"
	CodeTimeout        = "timeout"
	CodeLeadershipLost = "leadership_lost"
	CodeInternal       = "internal"
)

type Error struct {
	Code       string
	Outcome    Outcome
	Detail     string
	LeaderHint string
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

func BadRequest(detail string) *Error {
	return &Error{Code: CodeBadRequest, Outcome: Definite, Detail: detail}
}

func Unavailable(detail string) *Error {
	return &Error{Code: CodeUnavailable, Outcome: Definite, Detail: detail}
}

func NotLeader(leaderHint string) *Error {
	return &Error{Code: CodeNotLeader, Outcome: Definite, LeaderHint: leaderHint}
}

func Timeout() *Error {
	return &Error{Code: CodeTimeout, Outcome: Unknown}
}

func LeadershipLost() *Error {
	return &Error{Code: CodeLeadershipLost, Outcome: Unknown}
}

/** AsError maps any error to a protocol error. Anything unrecognised is
    reported as unknown: claiming "definitely not applied" wrongly would make
    the checker report violations that never happened.
**/
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return Timeout()
	}
	return &Error{Code: CodeInternal, Outcome: Unknown, Detail: err.Error()}
}
