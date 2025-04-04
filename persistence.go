package graft

import (
	"errors"

	"github.com/mizosoft/graft/pb"
)

var errEntryIndexOutOfRange = errors.New("entry index out of range")

type Persistence interface {
	GetState() *pb.PersistedState

	SetState(state *pb.PersistedState) error

	Append(state *pb.PersistedState, entries []*pb.LogEntry) (int, error)

	TruncatEntriesFrom(index int) error

	EntryCount() int

	GetEntry(index int) (*pb.LogEntry, error)

	GetEntryTerm(index int) (int, error)

	GetEntriesFrom(index int) ([]*pb.LogEntry, error)

	HeadEntry() (*pb.LogEntry, int, error)

	TailEntry() (*pb.LogEntry, int, error)

	Close() error
}

func NullPersistence() Persistence {
	return &nullPersistence{}
}

type nullPersistence struct {
	log   []*pb.LogEntry
	state *pb.PersistedState
}

func (p *nullPersistence) GetState() *pb.PersistedState {
	return p.state
}

func (p *nullPersistence) SetState(state *pb.PersistedState) error {
	p.state = state
	return nil
}

func (p *nullPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int, error) {
	p.state = state
	p.log = append(p.log, entries...)
	return len(p.log), nil
}

func (p *nullPersistence) TruncatEntriesFrom(index int) error {
	p.log = p.log[:index]
	return nil
}

func (p *nullPersistence) EntryCount() int {
	return len(p.log)
}

func (p *nullPersistence) GetEntry(index int) (*pb.LogEntry, error) {
	if index >= p.EntryCount() {
		return nil, errEntryIndexOutOfRange
	}
	return p.log[index], nil
}

func (p *nullPersistence) GetEntryTerm(index int) (int, error) {
	entry, err := p.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return int(entry.Term), nil
}

func (p *nullPersistence) GetEntriesFrom(index int) ([]*pb.LogEntry, error) {
	if index >= p.EntryCount() {
		return []*pb.LogEntry{}, nil
	}
	return p.log[index:], nil
}

func (p *nullPersistence) HeadEntry() (*pb.LogEntry, int, error) {
	return p.log[0], 0, nil
}

func (p *nullPersistence) TailEntry() (*pb.LogEntry, int, error) {
	lastIndex := len(p.log) - 1
	if lastIndex < 0 {
		return nil, -1, nil
	}
	return p.log[lastIndex], lastIndex, nil
}

func (p *nullPersistence) Close() error {
	return nil
}
