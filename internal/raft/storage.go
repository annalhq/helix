package raft

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type HardState struct {
	Term     uint64
	VotedFor string
}

/** Storage calls must be durable before they return: Raft replies to RPCs
    right after, and a restarted node must never forget a vote or an entry
**/
type Storage interface {
	Load() (HardState, []Entry, error)
	SaveHardState(HardState) error
	AppendEntries(from uint64, entries []Entry) error
}

type MemoryStorage struct {
	mu  sync.Mutex
	hs  HardState
	log []Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (s *MemoryStorage) Load() (HardState, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, append([]Entry(nil), s.log...), nil
}

func (s *MemoryStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	return nil
}

func (s *MemoryStorage) AppendEntries(from uint64, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if from == 0 || from-1 > uint64(len(s.log)) {
		return fmt.Errorf("append from %d leaves a gap after %d entries", from, len(s.log))
	}
	s.log = append(s.log[:from-1:from-1], entries...)
	return nil
}

const (
	walHeaderSize   = 8
	walMaxRecordLen = 64 << 20
)

var errTornRecord = errors.New("torn or corrupt wal record")

/** FileStorage is an append-only write-ahead log. Each record is
    [len uint32][crc32 uint32][gob payload] followed by fsync. A record with
    entries means "truncate the log to From-1, then append Entries"
**/
type FileStorage struct {
	f *os.File
}

type walRecord struct {
	HardState *HardState
	From      uint64
	Entries   []Entry
}

func OpenFileStorage(path string) (*FileStorage, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileStorage{f: f}, nil
}

/** A torn tail can only come from a write that never returned, so nothing
    was acknowledged on its behalf and it is safe to cut off
**/
func (s *FileStorage) Load() (HardState, []Entry, error) {
	var hs HardState
	var entries []Entry
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return hs, nil, err
	}
	rd := bufio.NewReader(s.f)
	var offset int64
	for {
		rec, n, err := readWALRecord(rd)
		if err == io.EOF {
			break
		}
		if errors.Is(err, errTornRecord) {
			if err := s.f.Truncate(offset); err != nil {
				return hs, nil, err
			}
			break
		}
		if err != nil {
			return hs, nil, err
		}
		if rec.HardState != nil {
			hs = *rec.HardState
		}
		if len(rec.Entries) > 0 {
			if rec.From == 0 || rec.From-1 > uint64(len(entries)) {
				return hs, nil, fmt.Errorf("wal record at offset %d appends from %d after %d entries", offset, rec.From, len(entries))
			}
			entries = append(entries[:rec.From-1], rec.Entries...)
		}
		offset += n
	}
	if _, err := s.f.Seek(offset, io.SeekStart); err != nil {
		return hs, nil, err
	}
	return hs, entries, nil
}

func (s *FileStorage) SaveHardState(hs HardState) error {
	return s.write(walRecord{HardState: &hs})
}

func (s *FileStorage) AppendEntries(from uint64, entries []Entry) error {
	return s.write(walRecord{From: from, Entries: entries})
}

func (s *FileStorage) Close() error {
	return s.f.Close()
}

func (s *FileStorage) write(rec walRecord) error {
	var buf bytes.Buffer
	buf.Write(make([]byte, walHeaderSize))
	if err := gob.NewEncoder(&buf).Encode(rec); err != nil {
		return err
	}
	b := buf.Bytes()
	payload := b[walHeaderSize:]
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(b[4:8], crc32.ChecksumIEEE(payload))
	if _, err := s.f.Write(b); err != nil {
		return err
	}
	return s.f.Sync()
}

func readWALRecord(rd io.Reader) (walRecord, int64, error) {
	var rec walRecord
	var header [walHeaderSize]byte
	if _, err := io.ReadFull(rd, header[:]); err != nil {
		if err == io.EOF {
			return rec, 0, io.EOF
		}
		return rec, 0, errTornRecord
	}
	size := binary.LittleEndian.Uint32(header[0:4])
	if size == 0 || size > walMaxRecordLen {
		return rec, 0, errTornRecord
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(rd, payload); err != nil {
		return rec, 0, errTornRecord
	}
	if crc32.ChecksumIEEE(payload) != binary.LittleEndian.Uint32(header[4:8]) {
		return rec, 0, errTornRecord
	}
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&rec); err != nil {
		return rec, 0, errTornRecord
	}
	return rec, int64(walHeaderSize) + int64(size), nil
}
