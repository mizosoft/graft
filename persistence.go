package graft

import (
	"fmt"
	"github.com/mizosoft/graft/pb"
)

type indexOutOfRangeError struct {
	Index int64
	Count int64
}

func (e indexOutOfRangeError) Error() string {
	s := fmt.Sprintf("index out of range: [%d]", e.Index)
	if e.Count >= 0 {
		s += fmt.Sprintf(" %d", e.Count)
	}
	return s
}

func indexOutOfRange(index int64) error {
	return indexOutOfRangeError{index, -1}
}

func indexOutOfRangeWithCount(index int64, count int64) error {
	return indexOutOfRangeError{index, count}
}

type Persistence interface {
	RetrieveState() *pb.PersistedState

	SaveState(state *pb.PersistedState) error

	Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error)

	AppendCommands(state *pb.PersistedState, commands [][]byte) ([]*pb.LogEntry, error)

	TruncateEntriesFrom(index int64) error

	TruncateEntriesTo(index int64) error

	EntryCount() int64

	GetEntry(index int64) (*pb.LogEntry, error)

	GetEntryTerm(index int64) (int64, error)

	GetEntriesFrom(index int64) ([]*pb.LogEntry, error)

	GetEntries(from, to int64) ([]*pb.LogEntry, error)

	FirstLogIndexAndTerm() (int64, int64)

	LastLogIndexAndTerm() (int64, int64)

	SaveSnapshot(snapshot Snapshot) error

	RetrieveSnapshot() (Snapshot, error)

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

func OpenWal(dir string, softSegmentSize int64) (Persistence, error) {
	return openWal(dir, softSegmentSize)
}

func NewSnapshot(metadata *pb.SnapshotMetadata, data []byte) Snapshot {
	return &snapshot{metadata: metadata, data: data}
}

type memoryPersistence struct {
	log      []*pb.LogEntry
	state    *pb.PersistedState
	snapshot Snapshot
}

func (p *memoryPersistence) RetrieveState() *pb.PersistedState {
	return p.state
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

func (p *memoryPersistence) AppendCommands(state *pb.PersistedState, commands [][]byte) ([]*pb.LogEntry, error) {
	entries := toLogEntries(state.CurrentTerm, int64(len(p.log)), commands)
	p.log = append(p.log, entries...)
	return entries, nil
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
		return nil, indexOutOfRangeWithCount(index, p.EntryCount())
	}
	return p.log[index], nil
}

func (p *memoryPersistence) GetEntryTerm(index int64) (int64, error) {
	entry, err := p.GetEntry(index)
	if err != nil {
		return -1, err
	}
	return entry.Term, nil
}

func (p *memoryPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	if index >= p.EntryCount() {
		return []*pb.LogEntry{}, indexOutOfRangeWithCount(index, p.EntryCount())
	}
	return p.log[index:], nil
}

func (p *memoryPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	firstIndex, _ := p.FirstLogIndexAndTerm()
	if from < firstIndex {
		return []*pb.LogEntry{}, indexOutOfRange(firstIndex)
	}

	lastIndex, _ := p.LastLogIndexAndTerm()
	if to > lastIndex {
		return []*pb.LogEntry{}, indexOutOfRange(firstIndex)
	}

	return p.log[from:to], nil
}

func (p *memoryPersistence) headEntry() (*pb.LogEntry, error) {
	if len(p.log) == 0 {
		return nil, nil
	}
	return p.log[0], nil
}

func (p *memoryPersistence) tailEntry() (*pb.LogEntry, error) {
	lastIndex := len(p.log) - 1
	if lastIndex < 0 {
		return nil, nil
	}
	return p.log[lastIndex], nil
}

func (p *memoryPersistence) FirstLogIndexAndTerm() (int64, int64) {
	entry, _ := p.headEntry()
	if entry == nil {
		return -1, -1
	}
	return entry.Index, entry.Term
}

func (p *memoryPersistence) LastLogIndexAndTerm() (int64, int64) {
	entry, _ := p.tailEntry()
	if entry == nil {
		return -1, -1
	}
	return entry.Index, entry.Term
}

func (p *memoryPersistence) SaveSnapshot(snap Snapshot) error {
	p.snapshot = snap
	return nil
}

func (p *memoryPersistence) RetrieveSnapshot() (Snapshot, error) {
	return p.snapshot, nil
}

func (p *memoryPersistence) Close() error {
	return nil
}
