package graft

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/mizosoft/graft/pb"
	"io"
)

var (
	ErrClosed           = errors.New("closed")
	ErrLargeRecord      = errors.New("record is too large")
	ErrCorrupt          = errors.New("log is corrupt")
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrNoSuchSnapshot   = errors.New("no such snapshot")
)

type IndexOutOfRangeError struct {
	Index int64
	Count int64
}

func (e IndexOutOfRangeError) Error() string {
	s := fmt.Sprintf("index out of range: [%d]", e.Index)
	if e.Count >= 0 {
		s += fmt.Sprintf(" %d", e.Count)
	}
	return s
}

func IndexOutOfRange(index int64) error {
	return IndexOutOfRangeError{index, -1}
}

func IndexOutOfRangeWithCount(index int64, count int64) error {
	return IndexOutOfRangeError{index, count}
}

// TODO segregate log/state/snapshot persistence.
type Persistence interface {
	io.Closer

	RetrieveState() (*pb.PersistedState, error)

	SaveState(state *pb.PersistedState) error

	Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error)

	TruncateEntriesFrom(index int64) error

	TruncateEntriesTo(index int64) error

	EntryCount() int64

	GetEntry(index int64) (*pb.LogEntry, error)

	GetEntryTerm(index int64) (int64, error)

	GetEntries(from, to int64) ([]*pb.LogEntry, error)

	GetEntriesFrom(index int64) ([]*pb.LogEntry, error)

	FirstEntryIndex() (int64, error)

	LastEntryIndex() (int64, error)

	LastSnapshotMetadata() (*pb.SnapshotMetadata, error)

	OpenSnapshot(metadata *pb.SnapshotMetadata) (Snapshot, error)

	NewSnapshot(metadata *pb.SnapshotMetadata) (SnapshotWriter, error)
}

type Snapshot interface {
	Metadata() *pb.SnapshotMetadata

	Data() []byte
}

type SnapshotWriter interface {
	io.WriterAt
	io.Closer

	Commit() (*pb.SnapshotMetadata, error)
}

type snapshot struct {
	metadata *pb.SnapshotMetadata
	data     []byte
}

func (s *snapshot) Metadata() *pb.SnapshotMetadata {
	return s.metadata
}

func (s *snapshot) Data() []byte {
	return s.data
}

func NewSnapshot(metadata *pb.SnapshotMetadata, data []byte) Snapshot {
	return &snapshot{metadata, data}
}

func MemoryPersistence() Persistence {
	return &memoryPersistence{}
}

type memoryPersistence struct {
	log      []*pb.LogEntry
	state    *pb.PersistedState
	snapshot Snapshot
}

func (m *memoryPersistence) RetrieveState() (*pb.PersistedState, error) {
	return m.state, nil
}

func (m *memoryPersistence) SaveState(state *pb.PersistedState) error {
	m.state = state
	return nil
}

func (m *memoryPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	m.state = state
	nextIndex := len(m.log)
	for _, entry := range entries {
		entry.Index = int64(nextIndex)
		nextIndex++
	}
	m.log = append(m.log, entries...)
	return int64(nextIndex), nil
}

func (m *memoryPersistence) TruncateEntriesFrom(index int64) error {
	m.log = m.log[:index]
	return nil
}

func (m *memoryPersistence) TruncateEntriesTo(index int64) error {
	m.log = m.log[index+1:]
	return nil
}

func (m *memoryPersistence) EntryCount() int64 {
	return int64(len(m.log))
}

func (m *memoryPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	if index >= m.EntryCount() {
		return nil, IndexOutOfRangeWithCount(index, m.EntryCount())
	}
	return m.log[index], nil
}

func (m *memoryPersistence) GetEntryTerm(index int64) (int64, error) {
	e, err := m.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (m *memoryPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	if index >= m.EntryCount() {
		panic(IndexOutOfRangeWithCount(index, m.EntryCount()))
	}
	return m.log[index:], nil
}

func (m *memoryPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	firstIndex, _ := m.FirstEntryIndex()
	if from < firstIndex {
		return nil, IndexOutOfRange(firstIndex)
	}

	lastIndex, _ := m.LastEntryIndex()
	if to > lastIndex {
		return nil, IndexOutOfRange(firstIndex)
	}

	return m.log[from:to], nil
}

func (m *memoryPersistence) headEntry() *pb.LogEntry {
	if len(m.log) == 0 {
		return nil
	}
	return m.log[0]
}

func (m *memoryPersistence) tailEntry() *pb.LogEntry {
	lastIndex := len(m.log) - 1
	if lastIndex < 0 {
		return nil
	}
	return m.log[lastIndex]
}

func (m *memoryPersistence) FirstEntryIndex() (int64, error) {
	entry := m.headEntry()
	if entry == nil {
		return -1, nil
	}
	return entry.Index, nil
}

func (m *memoryPersistence) LastEntryIndex() (int64, error) {
	entry := m.tailEntry()
	if entry == nil {
		return -1, nil
	}
	return entry.Index, nil
}

func (m *memoryPersistence) LastSnapshotMetadata() (*pb.SnapshotMetadata, error) {
	if m.snapshot == nil {
		return nil, nil
	}
	return m.snapshot.Metadata(), nil
}

func (m *memoryPersistence) OpenSnapshot(metadata *pb.SnapshotMetadata) (Snapshot, error) {
	if m.snapshot == nil {
		return nil, ErrNoSuchSnapshot
	}
	if metadata.LastAppliedIndex != m.snapshot.Metadata().LastAppliedIndex ||
		metadata.LastAppliedTerm != m.snapshot.Metadata().LastAppliedTerm {
		return nil, ErrNoSuchSnapshot
	}
	return m.snapshot, nil
}

func (m *memoryPersistence) NewSnapshot(metadata *pb.SnapshotMetadata) (SnapshotWriter, error) {
	return &memorySnapshotWriter{m: m, metadata: metadata, buffer: &bytes.Buffer{}}, nil
}

type memorySnapshotWriter struct {
	m        *memoryPersistence
	metadata *pb.SnapshotMetadata
	buffer   *bytes.Buffer
	closed   bool
}

func (w *memorySnapshotWriter) WriteAt(p []byte, off int64) (int, error) {
	if w.closed {
		return 0, ErrClosed
	}

	if off < 0 || off > int64(w.buffer.Len()) {
		return 0, ErrOffsetOutOfRange
	}

	n := 0
	if off < int64(w.buffer.Len()) { // Handle backwards-writing cases.
		n += copy(w.buffer.Bytes()[off:], p)
	}
	w.buffer.Write(p[n:])
	return len(p), nil
}

func (w *memorySnapshotWriter) Commit() (*pb.SnapshotMetadata, error) {
	if w.closed {
		return nil, ErrClosed
	}

	w.metadata.Size = int64(w.buffer.Len())
	snapshot := NewSnapshot(w.metadata, w.buffer.Bytes())
	w.m.snapshot = snapshot
	w.closed = true
	return w.metadata, nil
}

func (w *memorySnapshotWriter) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	return nil
}

func (w *memoryPersistence) Close() error {
	return nil
}
