package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func entry(term uint64, cmd string) Entry { return Entry{Term: term, Command: []byte(cmd)} }

func writeFixture(t *testing.T, s Storage) {
	t.Helper()
	must(t, s.SaveHardState(HardState{Term: 1, VotedFor: "a"}))
	must(t, s.AppendEntries(1, []Entry{entry(1, "x"), entry(1, "y"), entry(1, "z")}))
	must(t, s.SaveHardState(HardState{Term: 2}))
	must(t, s.AppendEntries(3, []Entry{entry(2, "z2")}))
}

var fixtureHardState = HardState{Term: 2}
var fixtureEntries = []Entry{entry(1, "x"), entry(1, "y"), entry(2, "z2")}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func checkLoad(t *testing.T, s Storage, wantHS HardState, want []Entry) {
	t.Helper()
	hs, entries, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != wantHS || !reflect.DeepEqual(entries, want) {
		t.Fatalf("Load() = %+v %v, want %+v %v", hs, entries, wantHS, want)
	}
}

func TestMemoryStorage(t *testing.T) {
	s := NewMemoryStorage()
	writeFixture(t, s)
	checkLoad(t, s, fixtureHardState, fixtureEntries)
	if err := s.AppendEntries(9, []Entry{entry(2, "gap")}); err == nil {
		t.Fatal("append leaving a gap should fail")
	}
}

func TestFileStorageSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	s, err := OpenFileStorage(path)
	must(t, err)
	writeFixture(t, s)
	must(t, s.Close())

	s, err = OpenFileStorage(path)
	must(t, err)
	defer s.Close()
	checkLoad(t, s, fixtureHardState, fixtureEntries)
}

func TestFileStorageDropsTornTailAndKeepsWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	s, err := OpenFileStorage(path)
	must(t, err)
	writeFixture(t, s)
	must(t, s.Close())

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	must(t, err)
	_, err = f.Write([]byte{0x20, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 1, 2, 3})
	must(t, err)
	must(t, f.Close())

	s, err = OpenFileStorage(path)
	must(t, err)
	checkLoad(t, s, fixtureHardState, fixtureEntries)
	must(t, s.AppendEntries(4, []Entry{entry(2, "after-crash")}))
	must(t, s.Close())

	s, err = OpenFileStorage(path)
	must(t, err)
	defer s.Close()
	checkLoad(t, s, fixtureHardState, append(fixtureEntries, entry(2, "after-crash")))
}
