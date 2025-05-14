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

	SaveState(state *pb.PersistedState)

	Append(state *pb.PersistedState, entries []*pb.LogEntry) int64

	TruncateEntriesFrom(index int64)

	TruncateEntriesTo(index int64)

	EntryCount() int64

	GetEntry(index int64) *pb.LogEntry

	GetEntryTerm(index int64) int64

	GetEntriesFrom(index int64) []*pb.LogEntry

	GetEntries(from, to int64) []*pb.LogEntry

	FirstLogIndexAndTerm() (int64, int64)

	LastLogIndexAndTerm() (int64, int64)

	SaveSnapshot(snapshot Snapshot)

	RetrieveSnapshot() Snapshot

	SnapshotMetadata() *pb.SnapshotMetadata

	Close()
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

func (p *memoryPersistence) SaveState(state *pb.PersistedState) {
	p.state = state
}

func (p *memoryPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) int64 {
	p.state = state
	nextIndex := len(p.log)
	for _, entry := range entries {
		entry.Index = int64(nextIndex)
		nextIndex++
	}
	p.log = append(p.log, entries...)
	return int64(nextIndex)
}

func (p *memoryPersistence) TruncateEntriesFrom(index int64) {
	p.log = p.log[:index]
}

func (p *memoryPersistence) TruncateEntriesTo(index int64) {
	p.log = p.log[index+1:]
}

func (p *memoryPersistence) EntryCount() int64 {
	return int64(len(p.log))
}

func (p *memoryPersistence) GetEntry(index int64) *pb.LogEntry {
	if index >= p.EntryCount() {
		panic(indexOutOfRangeWithCount(index, p.EntryCount()))
	}
	return p.log[index]
}

func (p *memoryPersistence) GetEntryTerm(index int64) int64 {
	return p.GetEntry(index).Term
}

func (p *memoryPersistence) GetEntriesFrom(index int64) []*pb.LogEntry {
	if index >= p.EntryCount() {
		panic(indexOutOfRangeWithCount(index, p.EntryCount()))
	}
	return p.log[index:]
}

func (p *memoryPersistence) GetEntries(from, to int64) []*pb.LogEntry {
	firstIndex, _ := p.FirstLogIndexAndTerm()
	if from < firstIndex {
		panic(indexOutOfRange(firstIndex))
	}

	lastIndex, _ := p.LastLogIndexAndTerm()
	if to > lastIndex {
		panic(indexOutOfRange(firstIndex))
	}

	return p.log[from:to]
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

func (p *memoryPersistence) FirstLogIndexAndTerm() (int64, int64) {
	entry := p.headEntry()
	if entry == nil {
		return -1, -1
	}
	return entry.Index, entry.Term
}

func (p *memoryPersistence) LastLogIndexAndTerm() (int64, int64) {
	entry := p.tailEntry()
	if entry == nil {
		return -1, -1
	}
	return entry.Index, entry.Term
}

func (p *memoryPersistence) SaveSnapshot(snap Snapshot) {
	p.snapshot = snap
}

func (p *memoryPersistence) RetrieveSnapshot() Snapshot {
	return p.snapshot
}

func (p *memoryPersistence) SnapshotMetadata() *pb.SnapshotMetadata {
	return p.snapshot.Metadata()
}

func (p *memoryPersistence) Close() {
}
