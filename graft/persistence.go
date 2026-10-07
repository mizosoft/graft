package graft

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"slices"
	"sync"

	"github.com/mizosoft/graft/pb"
)

var (
	ErrClosed           = errors.New("closed")
	ErrLargeRecord      = errors.New("record is too large")
	ErrCorrupt          = errors.New("log is corrupt")
	ErrOffsetOutOfRange = errors.New("offset out of range")
	ErrNoSuchSnapshot   = errors.New("no such snapshot")
	ErrSnapOutOfDate    = errors.New("snapshot is out of date")
)

type IndexOutOfRangeError struct {
	Index int64
	Count int64
}

func (e IndexOutOfRangeError) Error() string {
	s := fmt.Sprintf("index out of range: [%d]", e.Index)
	if e.Count >= 0 {
		s += fmt.Sprintf("with count %d", e.Count)
	}
	return s
}

func IndexOutOfRange(index int64) error {
	return IndexOutOfRangeError{index, -1}
}

func IndexOutOfRangeWithCount(index int64, count int64) error {
	return IndexOutOfRangeError{index, count}
}

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

	Metadata() *pb.SnapshotMetadata

	// Commit makes the snapshot the current one, unless it's not newer than the current one, in which case it's discarded
	// with ErrSnapOutOfDate.
	Commit() error
}

type memorySnapshot struct {
	metadata   *pb.SnapshotMetadata
	data       []byte
	lazyReader io.Reader
}

func (s *memorySnapshot) Metadata() *pb.SnapshotMetadata {
	return s.metadata
}

func (s *memorySnapshot) ReadAll() ([]byte, error) {
	return s.data, nil
}

func (s *memorySnapshot) Reader() io.Reader {
	if s.lazyReader == nil {
		s.lazyReader = bytes.NewReader(s.data)
	}
	return s.lazyReader
}

func (s *memorySnapshot) Close() error {
	return nil
}

func MemoryPersistence() Persistence {
	return &memoryPersistence{}
}

type memoryPersistence struct {
	mut       sync.RWMutex
	log       []*pb.LogEntry
	nextIndex int64
	state     *pb.PersistedState
	snapshot  *memorySnapshot
}

func (w *memoryPersistence) RetrieveState() (*pb.PersistedState, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.state, nil
}

func (w *memoryPersistence) SaveState(state *pb.PersistedState) error {
	w.mut.Lock()
	defer w.mut.Unlock()
	w.state = state
	return nil
}

func (w *memoryPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	w.mut.Lock()
	defer w.mut.Unlock()

	w.state = state
	for _, entry := range entries {
		entry.Index = w.nextIndex
		w.nextIndex++
	}
	w.log = append(w.log, entries...)
	return w.nextIndex, nil
}

func (w *memoryPersistence) TruncateEntriesFrom(index int64) error {
	w.mut.Lock()
	defer w.mut.Unlock()

	if firstIndex := w.firstIndex(); firstIndex >= 0 {
		if index <= firstIndex {
			w.log = nil
		} else if off := w.logOffset(index); off >= 0 {
			w.log = w.log[:off]
		} else {
			return IndexOutOfRange(index)
		}
	}
	if index < w.nextIndex {
		w.nextIndex = index
	}
	return nil
}

func (w *memoryPersistence) TruncateEntriesTo(index int64) error {
	w.mut.Lock()
	defer w.mut.Unlock()

	firstIndex := w.firstIndex()
	if firstIndex < 0 || index < firstIndex {
		return IndexOutOfRange(index)
	}
	if index >= w.lastIndex() {
		w.log = nil
		return nil
	}
	w.log = w.log[index-firstIndex+1:]
	return nil
}

func (w *memoryPersistence) EntryCount() int64 {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return int64(len(w.log))
}

func (w *memoryPersistence) firstIndex() int64 {
	if len(w.log) == 0 {
		return -1
	}
	return w.log[0].Index
}

func (w *memoryPersistence) lastIndex() int64 {
	if len(w.log) == 0 {
		return -1
	}
	return w.log[len(w.log)-1].Index
}

func (w *memoryPersistence) logOffset(index int64) int64 {
	firstIndex := w.firstIndex()
	if firstIndex < 0 || index < firstIndex {
		return -1
	}
	off := index - firstIndex
	if off >= int64(len(w.log)) {
		return -1
	}
	return off
}

func (w *memoryPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()

	off := w.logOffset(index)
	if off < 0 {
		return nil, IndexOutOfRangeWithCount(index, int64(len(w.log)))
	}
	return w.log[off], nil
}

func (w *memoryPersistence) GetEntryTerm(index int64) (int64, error) {
	e, err := w.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (w *memoryPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()

	off := w.logOffset(index)
	if off < 0 {
		return nil, IndexOutOfRangeWithCount(index, int64(len(w.log)))
	}
	return slices.Clone(w.log[off:]), nil
}

func (w *memoryPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	if from > to {
		return nil, fmt.Errorf("from (%d) must be smaller than or equal to (%d)", from, to)
	}

	w.mut.RLock()
	defer w.mut.RUnlock()

	fromOff := w.logOffset(from)
	if fromOff < 0 {
		return nil, IndexOutOfRange(from)
	}

	toOff := w.logOffset(to)
	if toOff < 0 {
		return nil, IndexOutOfRange(to)
	}
	return slices.Clone(w.log[fromOff : toOff+1]), nil
}

func (w *memoryPersistence) FirstEntryIndex() (int64, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.firstIndex(), nil
}

func (w *memoryPersistence) LastEntryIndex() (int64, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return w.lastIndex(), nil
}

func (w *memoryPersistence) LastSnapshotMetadata() (*pb.SnapshotMetadata, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()

	if w.snapshot == nil {
		return nil, nil
	}
	return w.snapshot.Metadata(), nil
}

func (w *memoryPersistence) OpenSnapshot(metadata *pb.SnapshotMetadata) (Snapshot, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()

	if w.snapshot == nil {
		return nil, ErrNoSuchSnapshot
	}
	if metadata.LastIncludedIndex != w.snapshot.Metadata().LastIncludedIndex ||
			metadata.LastIncludedTerm != w.snapshot.Metadata().LastIncludedTerm {
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

func (w *memorySnapshotWriter) Metadata() *pb.SnapshotMetadata {
	return w.metadata
}

func (w *memorySnapshotWriter) Commit() error {
	if w.closed {
		return ErrClosed
	}

	w.closed = true

	w.m.mut.Lock()
	defer w.m.mut.Unlock()

	if current := w.m.snapshot; current != nil && w.metadata.LastIncludedIndex <= current.Metadata().LastIncludedIndex {
		return ErrSnapOutOfDate
	}
	w.metadata.Size = int64(w.buffer.Len())
	w.m.snapshot = &memorySnapshot{
		metadata: w.metadata,
		data:     w.buffer.Bytes(),
	}
	return nil
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
	return fmt.Sprintf("snap_%d_%d.tmp", metadata.LastIncludedIndex, metadata.LastIncludedTerm)
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
	data, err := io.ReadAll(b.f)
	if err != nil {
		return nil, err
	}
	if b.metadata.Checksum != 0 && crc32.Checksum(data, snapshotCRCTable) != b.metadata.Checksum {
		return nil, ErrCorrupt
	}
	return data, nil
}

func (b *fileSnapshot) Reader() io.Reader {
	if b.metadata.Checksum == 0 {
		return b.f
	}
	return &checksumVerifyingReader{r: b.f, expected: b.metadata.Checksum}
}
