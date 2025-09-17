package graft

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mizosoft/graft/pb"
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

	CreateSnapshot(metadata *pb.SnapshotMetadata) (SnapshotWriter, error)
}

type Snapshot interface {
	io.Closer

	Metadata() *pb.SnapshotMetadata

	ReadAll() ([]byte, error)

	Reader() io.Reader
}

type SnapshotWriter interface {
	io.WriterAt
	io.Closer

	Commit() (*pb.SnapshotMetadata, error)
}

type snapshot struct {
	metadata   *pb.SnapshotMetadata
	data       []byte
	lazyReader io.Reader
}

func (s *snapshot) Metadata() *pb.SnapshotMetadata {
	return s.metadata
}

func (s *snapshot) ReadAll() ([]byte, error) {
	return s.data, nil
}

func (s *snapshot) Reader() io.Reader {
	if s.lazyReader == nil {
		s.lazyReader = bytes.NewReader(s.data)
	}
	return s.lazyReader
}

func (s *snapshot) Close() error {
	return nil
}

func NewMemorySnapshot(metadata *pb.SnapshotMetadata, data []byte) Snapshot {
	return &snapshot{
		metadata: metadata,
		data:     data,
	}
}

func MemoryPersistence() Persistence {
	return &memoryPersistence{}
}

type memoryPersistence struct {
	log      []*pb.LogEntry
	state    *pb.PersistedState
	snapshot Snapshot
}

func (w *memoryPersistence) RetrieveState() (*pb.PersistedState, error) {
	return w.state, nil
}

func (w *memoryPersistence) SaveState(state *pb.PersistedState) error {
	w.state = state
	return nil
}

func (w *memoryPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	w.state = state
	nextIndex := len(w.log)
	for _, entry := range entries {
		entry.Index = int64(nextIndex)
		nextIndex++
	}
	w.log = append(w.log, entries...)
	return int64(nextIndex), nil
}

func (w *memoryPersistence) TruncateEntriesFrom(index int64) error {
	w.log = w.log[:index]
	return nil
}

func (w *memoryPersistence) TruncateEntriesTo(index int64) error {
	w.log = w.log[index+1:]
	return nil
}

func (w *memoryPersistence) EntryCount() int64 {
	return int64(len(w.log))
}

func (w *memoryPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	if index >= w.EntryCount() {
		return nil, IndexOutOfRangeWithCount(index, w.EntryCount())
	}
	return w.log[index], nil
}

func (w *memoryPersistence) GetEntryTerm(index int64) (int64, error) {
	e, err := w.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (w *memoryPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	if index >= w.EntryCount() {
		panic(IndexOutOfRangeWithCount(index, w.EntryCount()))
	}
	return w.log[index:], nil
}

func (w *memoryPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	firstIndex, _ := w.FirstEntryIndex()
	if from < firstIndex {
		return nil, IndexOutOfRange(firstIndex)
	}

	lastIndex, _ := w.LastEntryIndex()
	if to > lastIndex {
		return nil, IndexOutOfRange(firstIndex)
	}

	return w.log[from:to], nil
}

func (w *memoryPersistence) headEntry() *pb.LogEntry {
	if len(w.log) == 0 {
		return nil
	}
	return w.log[0]
}

func (w *memoryPersistence) tailEntry() *pb.LogEntry {
	lastIndex := len(w.log) - 1
	if lastIndex < 0 {
		return nil
	}
	return w.log[lastIndex]
}

func (w *memoryPersistence) FirstEntryIndex() (int64, error) {
	entry := w.headEntry()
	if entry == nil {
		return -1, nil
	}
	return entry.Index, nil
}

func (w *memoryPersistence) LastEntryIndex() (int64, error) {
	entry := w.tailEntry()
	if entry == nil {
		return -1, nil
	}
	return entry.Index, nil
}

func (w *memoryPersistence) LastSnapshotMetadata() (*pb.SnapshotMetadata, error) {
	if w.snapshot == nil {
		return nil, nil
	}
	return w.snapshot.Metadata(), nil
}

func (w *memoryPersistence) OpenSnapshot(metadata *pb.SnapshotMetadata) (Snapshot, error) {
	if w.snapshot == nil {
		return nil, ErrNoSuchSnapshot
	}
	if metadata.LastAppliedIndex != w.snapshot.Metadata().LastAppliedIndex ||
		metadata.LastAppliedTerm != w.snapshot.Metadata().LastAppliedTerm {
		return nil, ErrNoSuchSnapshot
	}
	return w.snapshot, nil
}

func (w *memoryPersistence) CreateSnapshot(metadata *pb.SnapshotMetadata) (SnapshotWriter, error) {
	return &memorySnapshotWriter{m: w, metadata: metadata, buffer: &bytes.Buffer{}}, nil
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
	snapshot := NewMemorySnapshot(w.metadata, w.buffer.Bytes())
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

func SnapshotFilename(metadata *pb.SnapshotMetadata) string {
	return fmt.Sprintf("snap_%d_%d.tmp", metadata.LastAppliedIndex, metadata.LastAppliedTerm)
}

func NewFileSnapshot(metadata *pb.SnapshotMetadata, path string) (Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &fileSnapshot{f: f, metadata: metadata}, nil
}

type fileSnapshot struct {
	f        *os.File
	metadata *pb.SnapshotMetadata
	closed   bool
}

func (b *fileSnapshot) Close() error {
	return b.f.Close()
}

func (b *fileSnapshot) Metadata() *pb.SnapshotMetadata {
	return b.metadata
}

func (b *fileSnapshot) ReadAll() ([]byte, error) {
	return io.ReadAll(b.f)
}

func (b *fileSnapshot) Reader() io.Reader {
	return b.f
}
