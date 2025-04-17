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
	GetState() *pb.PersistedState

	SetState(state *pb.PersistedState) error

	Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error)

	AppendCommands(state *pb.PersistedState, commands [][]byte) ([]*pb.LogEntry, error)

	TruncateEntriesFrom(index int64) error

	EntryCount() int64

	GetEntry(index int64) (*pb.LogEntry, error)

	GetEntryTerm(index int64) (int64, error)

	GetEntriesFrom(index int64) ([]*pb.LogEntry, error)

	LastLogIndexAndTerm() (int64, int64)

	Close() error
}

func MemoryPersistence() Persistence {
	return &memoryPersistence{}
}

func OpenWal(dir string, softSegmentSize int64) (Persistence, error) {
	return openWal(dir, softSegmentSize)
}

type memoryPersistence struct {
	log   []*pb.LogEntry
	state *pb.PersistedState
}

func (p *memoryPersistence) GetState() *pb.PersistedState {
	return p.state
}

func (p *memoryPersistence) SetState(state *pb.PersistedState) error {
	p.state = state
	return nil
}

func (p *memoryPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	p.state = state
	nextIndex := len(p.log) - 1
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

func (p *memoryPersistence) HeadEntry() (*pb.LogEntry, error) {
	if len(p.log) == 0 {
		return nil, nil
	}
	return p.log[0], nil
}

func (p *memoryPersistence) TailEntry() (*pb.LogEntry, error) {
	lastIndex := len(p.log) - 1
	if lastIndex < 0 {
		return nil, nil
	}
	return p.log[lastIndex], nil
}

func (p *memoryPersistence) LastLogIndexAndTerm() (int64, int64) {
	entry, _ := p.TailEntry()
	if entry == nil {
		return -1, -1
	}
	return entry.Index, entry.Term
}

func (p *memoryPersistence) Close() error {
	return nil
}
