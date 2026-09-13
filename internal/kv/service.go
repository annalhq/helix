package kv

import (
	"context"
	"fmt"
)

type ReadMode string

const (
	ReadLocal ReadMode = "local"
	ReadLog   ReadMode = "log"
)

func ParseReadMode(s string) (ReadMode, error) {
	switch m := ReadMode(s); m {
	case ReadLocal, ReadLog:
		return m, nil
	}
	return "", fmt.Errorf("invalid read mode %q (want local or log)", s)
}

type Service struct {
	sm       *StateMachine
	readMode ReadMode
}

func NewService(sm *StateMachine, readMode ReadMode) *Service {
	return &Service{sm: sm, readMode: readMode}
}

func (s *Service) ReadMode() ReadMode { return s.readMode }

func (s *Service) Submit(ctx context.Context, cmd Command) (Result, error) {
	if err := cmd.Validate(); err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	return s.sm.Apply(cmd), nil
}
