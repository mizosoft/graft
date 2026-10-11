package graft

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
)

var (
	ErrClosed            = errors.New("closed")
	ErrLargeRecord       = errors.New("record is too large")
	ErrCorrupt           = errors.New("log is corrupt")
	ErrOffsetOutOfRange  = errors.New("offset out of range")
	ErrNoSuchSnapshot    = errors.New("no such snapshot")
	ErrSnapshotOutOfDate = errors.New("snapshot is out of date")
	ErrCompacted         = errors.New("index is compacted")
)

// CheckRange checks an entry range given log bounds. ErrCompacted is returned if the range starts before first, a panic
// occurs if it's invalid or ends past last. For an empty log, first indicates next index, and last is first-1.
func CheckRange(from, to, first, last int64) error {
	if from < 0 || from > to {
		panic(fmt.Sprintf("invalid index range [%d, %d]", from, to))
	}
	if to > last {
		panic(fmt.Sprintf("index range [%d, %d] ends past the last index %d", from, to, last))
	}
	if from < first {
		return ErrCompacted
	}
	return nil
}

type Persistence interface {
	io.Closer

	RetrieveState() (*pb.PersistedState, error)

	SaveState(state *pb.PersistedState) error

	Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error)

	// TruncateEntriesFrom removes the entries from given index to the end of the log. It returns ErrCompacted for an
	// index before the first entry, and panics on one past the last.
	TruncateEntriesFrom(index int64) error

	// TruncateEntriesTo compacts the entries up to the given index. An index before the first entry does nothing, and one
	// past the last deletes the log.
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

	// CreateSnapshot starts writing a snapshot, unless it's not newer than the current one, in which case
	// ErrSnapshotOutOfDate is returned.
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

	// Commit commits the snapshot, unless it's not newer than the current one, in which case it is discarded and
	// ErrSnapshotOutOfDate is returned
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

	first, last := w.bounds()
	if err := CheckRange(index, index, first, last); err != nil {
		return err
	}
	w.log = w.log[:index-first]
	w.nextIndex = index
	return nil
}

func (w *memoryPersistence) TruncateEntriesTo(index int64) error {
	w.mut.Lock()
	defer w.mut.Unlock()

	first, last := w.bounds()
	if index = min(index, last); index >= first {
		w.log = w.log[index-first+1:]
	}
	return nil
}

func (w *memoryPersistence) EntryCount() int64 {
	w.mut.RLock()
	defer w.mut.RUnlock()
	return int64(len(w.log))
}

// bounds returns the first and last index, first being where the log continues when it's empty.
func (w *memoryPersistence) bounds() (first, last int64) {
	if len(w.log) == 0 {
		return w.nextIndex, w.nextIndex - 1
	}
	return w.log[0].Index, w.nextIndex - 1
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

func (w *memoryPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()

	first, last := w.bounds()
	if err := CheckRange(index, index, first, last); err != nil {
		return nil, err
	}
	return w.log[index-first], nil
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

	first, last := w.bounds()
	if err := CheckRange(index, index, first, last); err != nil {
		return nil, err
	}
	return slices.Clone(w.log[index-first:]), nil
}

func (w *memoryPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	w.mut.RLock()
	defer w.mut.RUnlock()

	first, last := w.bounds()
	if err := CheckRange(from, to, first, last); err != nil {
		return nil, err
	}
	return slices.Clone(w.log[from-first : to-first+1]), nil
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
	w.mut.Lock()
	defer w.mut.Unlock()

	if current := w.snapshot; current != nil && current.Metadata().LastIncludedIndex >= metadata.LastIncludedIndex {
		return nil, ErrSnapshotOutOfDate
	}
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
		return ErrSnapshotOutOfDate
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

// FileSnapshotter manages opening, creating and committing snapshots for a disk-based Persistence that stores snapshots
// as files in a directory. Saving metadata is left to the Persistence implementation.
type FileSnapshotter struct {
	dir            string
	lock           *sync.RWMutex
	getMetadata    func() (*pb.SnapshotMetadata, error)
	commitMetadata func(*pb.SnapshotMetadata) error
	logger         *zap.Logger
	tempSeq        atomic.Uint64
}

// NewFileSnapshotter creates a FileSnapshotter. `getMetadata` and `commitMetadata` are provided by the Persistence to
// get and commit the current snapshot's metadata respectively. getMetadata is called with the given lock held, for
// reading at least, and commitMetadata with it held for writing. A nil logger is a no-op.
func NewFileSnapshotter(
		dir string, lock *sync.RWMutex, getMetadata func() (*pb.SnapshotMetadata, error),
		commitMetadata func(*pb.SnapshotMetadata) error, logger *zap.Logger) *FileSnapshotter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &FileSnapshotter{
		dir:            dir,
		lock:           lock,
		getMetadata:    getMetadata,
		commitMetadata: commitMetadata,
		logger:         logger,
	}
}

// Create creates a SnapshotWriter for a snapshot represented by the given metadata, returning ErrSnapshotOutOfDate if
// the given metadata is not newer than the committed snapshot's metadata.
func (s *FileSnapshotter) Create(metadata *pb.SnapshotMetadata) (SnapshotWriter, error) {
	s.lock.RLock()
	current, err := s.getMetadata()
	s.lock.RUnlock()
	if err != nil {
		return nil, err
	}
	if current != nil && current.LastIncludedIndex >= metadata.LastIncludedIndex {
		return nil, ErrSnapshotOutOfDate
	}

	// Name snaps uniquely as two writers can hold the same metadata, e.g. a local snapshot and one installed by the leader.
	name := fmt.Sprintf("snap_%d_%d.%d.tmp", metadata.LastIncludedIndex, metadata.LastIncludedTerm, s.tempSeq.Add(1))
	f, err := os.Create(path.Join(s.dir, name))
	if err != nil {
		return nil, err
	}
	return &fileSnapshotWriter{files: s, metadata: metadata, f: f, hashValid: true}, nil
}

// Open opens the snapshot matching the given metadata, returning ErrNoSuchSnapshot if no such snapshot is found.
func (s *FileSnapshotter) Open(metadata *pb.SnapshotMetadata) (Snapshot, error) {
	s.lock.RLock()
	defer s.lock.RUnlock() // A commit can't delete the file between the check and the open.

	current, err := s.getMetadata()
	if err != nil {
		return nil, err
	}
	if current == nil || current.LastIncludedIndex != metadata.LastIncludedIndex ||
			current.LastIncludedTerm != metadata.LastIncludedTerm {
		return nil, ErrNoSuchSnapshot
	}
	f, err := os.Open(path.Join(s.dir, snapshotFilename(metadata)))
	if err != nil {
		return nil, err
	}
	return &fileSnapshot{f: f, metadata: metadata}, nil
}

func snapshotFilename(metadata *pb.SnapshotMetadata) string {
	return fmt.Sprintf("snap_%d_%d.snap", metadata.LastIncludedIndex, metadata.LastIncludedTerm)
}

type fileSnapshotWriter struct {
	files      *FileSnapshotter
	f          *os.File
	metadata   *pb.SnapshotMetadata
	lastOffset int64
	closed     bool
	h          uint32 // Running CRC32 of [0, lastOffset), only valid when hashValid is true.
	hashValid  bool   // Set to false when writes jump back.
}

func (s *fileSnapshotWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if s.closed {
		return 0, ErrClosed
	}

	if off < 0 || off > s.lastOffset {
		return 0, ErrOffsetOutOfRange
	}
	n, err = s.f.WriteAt(p, off)
	if err == nil {
		if s.hashValid {
			if off == s.lastOffset {
				s.h = crc32.Update(s.h, snapshotCRCTable, p[:n])
			} else {
				s.hashValid = false
			}
		}
		s.lastOffset = max(s.lastOffset, off+int64(n))
	}
	return
}

func (s *fileSnapshotWriter) computeChecksum() (uint32, error) {
	if s.hashValid {
		return s.h, nil
	}

	// A write jumped back over hashed bytes, so hash the file again.
	h := uint32(0)
	r := newBufferedReader(io.NewSectionReader(s.f, 0, s.lastOffset))
	buf := make([]byte, bufferSize)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			h = crc32.Update(h, snapshotCRCTable, buf[:n])
		}
		if err == io.EOF {
			return h, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

func (s *fileSnapshotWriter) Metadata() *pb.SnapshotMetadata {
	return s.metadata
}

func (s *fileSnapshotWriter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.f.Close(), os.Remove(s.f.Name()))
}

func (s *fileSnapshotWriter) Commit() error {
	if s.closed {
		return ErrClosed
	}
	s.closed = true

	fpath := s.f.Name()
	fileClosed := false
	committed := false
	defer func() {
		if committed {
			return
		}

		var closeErr error
		if !fileClosed {
			closeErr = s.f.Close()
		}
		if err := errors.Join(closeErr, os.Remove(fpath)); err != nil {
			s.files.logger.Error("Failed to close and/or delete snapshot file", zap.Error(err))
		}
	}()

	if err := s.f.Sync(); err != nil {
		return err
	}

	checksum, err := s.computeChecksum()
	if err != nil {
		return err
	}

	fileClosed = true // Even on error: a second Close would only report the file as already closed.
	if err := s.f.Close(); err != nil {
		return err
	}

	s.files.lock.Lock()
	defer s.files.lock.Unlock()

	// Reject snapshot if it moves backwards.
	prevMetadata, err := s.files.getMetadata()
	if err != nil {
		return err
	}
	if prevMetadata != nil && s.metadata.LastIncludedIndex <= prevMetadata.LastIncludedIndex {
		return ErrSnapshotOutOfDate
	}

	committedFpath := path.Join(s.files.dir, snapshotFilename(s.metadata))
	if err := os.Rename(fpath, committedFpath); err != nil {
		return err
	}
	fpath = committedFpath

	s.metadata.Size = s.lastOffset
	s.metadata.Checksum = checksum
	if err := s.files.commitMetadata(s.metadata); err != nil {
		return err
	}
	committed = true

	// Delete previous snapshot.
	if prevMetadata != nil {
		if err := os.Remove(path.Join(s.files.dir, snapshotFilename(prevMetadata))); err != nil {
			s.files.logger.Error("Failed to delete previous snapshot file", zap.Error(err))
		}
	}

	return nil
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
