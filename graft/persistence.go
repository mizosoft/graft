package graft

import (
	"errors"
	"fmt"
	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
)

var (
	ErrClosed      = errors.New("closed")
	ErrLargeRecord = errors.New("record is too large")
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

type Persistence interface {
	RetrieveState() (*pb.PersistedState, error)

	SaveState(state *pb.PersistedState) error

	Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error)

	TruncateEntriesFrom(index int64) error

	TruncateEntriesTo(index int64) error

	EntryCount() int64

	GetEntry(index int64) (*pb.LogEntry, error)

	GetEntryTerm(index int64) (int64, error)

	GetEntriesFrom(index int64) ([]*pb.LogEntry, error)

	GetEntries(from, to int64) ([]*pb.LogEntry, error)

	FirstEntryIndex() (int64, error)

	LastEntryIndex() (int64, error)

	SaveSnapshot(snapshot Snapshot) error

	RetrieveSnapshot() (Snapshot, error)

	SnapshotMetadata() (*pb.SnapshotMetadata, error)

	Close() error
}

type Snapshot interface {
	Metadata() *pb.SnapshotMetadata

	Data() []byte
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

func MemoryPersistence() Persistence {
	return &memoryPersistence{}
}

func OpenWal(dir string, softSegmentSize int64, suffixCacheSize int64, logger *zap.Logger) (Persistence, error) {
	return openCachedWal(dir, softSegmentSize, suffixCacheSize, logger)
}

func NewSnapshot(metadata *pb.SnapshotMetadata, data []byte) Snapshot {
	return &snapshot{metadata: metadata, data: data}
}

type memoryPersistence struct {
	log      []*pb.LogEntry
	state    *pb.PersistedState
	snapshot Snapshot
}

func (p *memoryPersistence) RetrieveState() (*pb.PersistedState, error) {
	return p.state, nil
}

func (p *memoryPersistence) SaveState(state *pb.PersistedState) error {
	p.state = state
	return nil
}

func (p *memoryPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	p.state = state
	nextIndex := len(p.log)
	for _, entry := range entries {
		entry.Index = int64(nextIndex)
		nextIndex++
	}
	p.log = append(p.log, entries...)
	return int64(nextIndex), nil
}

func (p *memoryPersistence) TruncateEntriesFrom(index int64) error {
	p.log = p.log[:index]
	return nil
}

func (p *memoryPersistence) TruncateEntriesTo(index int64) error {
	p.log = p.log[index+1:]
	return nil
}

func (p *memoryPersistence) EntryCount() int64 {
	return int64(len(p.log))
}

func (p *memoryPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	if index >= p.EntryCount() {
		return nil, IndexOutOfRangeWithCount(index, p.EntryCount())
	}
	return p.log[index], nil
}

func (p *memoryPersistence) GetEntryTerm(index int64) (int64, error) {
	e, err := p.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (p *memoryPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	if index >= p.EntryCount() {
		panic(IndexOutOfRangeWithCount(index, p.EntryCount()))
	}
	return p.log[index:], nil
}

func (p *memoryPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	firstIndex, _ := p.FirstEntryIndex()
	if from < firstIndex {
		return nil, IndexOutOfRange(firstIndex)
	}

	lastIndex, _ := p.LastEntryIndex()
	if to > lastIndex {
		return nil, IndexOutOfRange(firstIndex)
	}

	return p.log[from:to], nil
}

func (p *memoryPersistence) headEntry() *pb.LogEntry {
	if len(p.log) == 0 {
		return nil
	}
	return p.log[0]
}

func (p *memoryPersistence) tailEntry() *pb.LogEntry {
	lastIndex := len(p.log) - 1
	if lastIndex < 0 {
		return nil
	}
	return p.log[lastIndex]
}

func (p *memoryPersistence) FirstEntryIndex() (int64, error) {
	entry := p.headEntry()
	if entry == nil {
		return -1, nil
	}
	return entry.Index, nil
}

func (p *memoryPersistence) LastEntryIndex() (int64, error) {
	entry := p.tailEntry()
	if entry == nil {
		return -1, nil
	}
	return entry.Index, nil
}

func (p *memoryPersistence) SaveSnapshot(snap Snapshot) error {
	p.snapshot = snap
	return nil
}

func (p *memoryPersistence) RetrieveSnapshot() (Snapshot, error) {
	return p.snapshot, nil
}

func (p *memoryPersistence) SnapshotMetadata() (*pb.SnapshotMetadata, error) {
	return p.snapshot.Metadata(), nil
}

func (p *memoryPersistence) Close() error {
	return nil
}
