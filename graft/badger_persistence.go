package graft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
)

const (
	logEntryPrefix  = "log:"
	stateKey        = "state"
	snapshotMetaKey = "snapshot:meta"
	snapshotDataKey = "snapshot:data"
	firstIndexKey   = "firstIndex"
	lastIndexKey    = "lastIndex"
)

type badgerPersistence struct {
	db *badger.DB
}

func OpenBadgerPersistence(dir string) (Persistence, error) {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	opts.SyncWrites = true

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &badgerPersistence{
		db: db,
	}, nil
}

func (p *badgerPersistence) RetrieveState() *pb.PersistedState {
	var state *pb.PersistedState
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(stateKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}

		return item.Value(func(val []byte) error {
			state = &pb.PersistedState{}
			return proto.Unmarshal(val, state)
		})
	})

	if err != nil {
		panic(err)
	}
	return state
}

func (p *badgerPersistence) SaveState(state *pb.PersistedState) {
	data, err := proto.Marshal(state)
	if err != nil {
		panic(err)
	}

	err = p.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(stateKey), data)
	})

	if err != nil {
		panic(err)
	}
}

func (p *badgerPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) int64 {
	p.SaveState(state)

	if len(entries) == 0 {
		return p.EntryCount()
	}

	var nextIndex int64
	err := p.db.Update(func(txn *badger.Txn) error {
		// Get current last index.
		lastIndex := p.getLastIndexInTxn(txn)
		nextIndex = lastIndex + 1

		// Set first index if this is the first entry.
		if lastIndex == -1 {
			if err := p.setFirstIndexInTxn(txn, nextIndex); err != nil {
				return err
			}
		}

		for _, entry := range entries {
			entry.Index = nextIndex
			data, err := proto.Marshal(entry)
			if err != nil {
				return err
			}

			key := p.logEntryKey(nextIndex)
			if err := txn.Set(key, data); err != nil {
				return err
			}
			nextIndex++
		}

		// Update last index.
		return p.setLastIndexInTxn(txn, nextIndex-1)
	})

	if err != nil {
		panic(err)
	}
	return nextIndex
}

func (p *badgerPersistence) TruncateEntriesFrom(index int64) {
	err := p.db.Update(func(txn *badger.Txn) error {
		lastIndex := p.getLastIndexInTxn(txn)
		if lastIndex < index {
			return nil
		}

		// Delete entries from index onwards.
		for i := index; i <= lastIndex; i++ {
			key := p.logEntryKey(i)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		newLastIndex := index - 1
		if newLastIndex < p.getFirstIndexInTxn(txn) {
			// No entries left
			if err := p.setFirstIndexInTxn(txn, -1); err != nil {
				return err
			}
			return p.setLastIndexInTxn(txn, -1)
		}
		return p.setLastIndexInTxn(txn, newLastIndex)
	})

	if err != nil {
		panic(err)
	}
}

func (p *badgerPersistence) TruncateEntriesTo(index int64) {
	err := p.db.Update(func(txn *badger.Txn) error {
		firstIndex := p.getFirstIndexInTxn(txn)
		if firstIndex > index {
			return IndexOutOfRange(index)
		}

		lastIndex := p.getLastIndexInTxn(txn)
		if lastIndex < index {
			return IndexOutOfRange(index)
		}

		// Delete entries up to and including index.
		for i := firstIndex; i <= index; i++ {
			key := p.logEntryKey(i)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		newFirstIndex := index + 1
		if newFirstIndex > lastIndex {
			// No entries left
			if err := p.setFirstIndexInTxn(txn, -1); err != nil {
				return err
			}
			return p.setLastIndexInTxn(txn, -1)
		}
		return p.setFirstIndexInTxn(txn, newFirstIndex)
	})

	if err != nil {
		panic(err)
	}
}

func (p *badgerPersistence) EntryCount() int64 {
	var count int64
	err := p.db.View(func(txn *badger.Txn) error {
		firstIndex := p.getFirstIndexInTxn(txn)
		lastIndex := p.getLastIndexInTxn(txn)

		if firstIndex == -1 || lastIndex == -1 {
			count = 0
		} else {
			count = lastIndex - firstIndex + 1
		}

		return nil
	})

	if err != nil {
		panic(err)
	}
	return count
}

func (p *badgerPersistence) GetEntry(index int64) *pb.LogEntry {
	var entry *pb.LogEntry
	err := p.db.View(func(txn *badger.Txn) error {
		key := p.logEntryKey(index)
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return IndexOutOfRange(index)
			}
			return err
		}

		return item.Value(func(val []byte) error {
			entry = &pb.LogEntry{}
			return proto.Unmarshal(val, entry)
		})
	})

	if err != nil {
		panic(err)
	}
	return entry
}

func (p *badgerPersistence) GetEntryTerm(index int64) int64 {
	return p.GetEntry(index).Term
}

func (p *badgerPersistence) GetEntriesFrom(index int64) []*pb.LogEntry {
	var entries []*pb.LogEntry
	err := p.db.View(func(txn *badger.Txn) error {
		lastIndex := p.getLastIndexInTxn(txn)
		if index > lastIndex {
			panic(IndexOutOfRange(index))
		}

		for i := index; i <= lastIndex; i++ {
			key := p.logEntryKey(i)
			item, err := txn.Get(key)
			if err != nil {
				return err
			}

			err = item.Value(func(val []byte) error {
				entry := &pb.LogEntry{}
				if err := proto.Unmarshal(val, entry); err != nil {
					return err
				}
				entries = append(entries, entry)
				return nil
			})
			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		panic(err)
	}
	return entries
}

func (p *badgerPersistence) GetEntries(from, to int64) []*pb.LogEntry {
	var entries []*pb.LogEntry
	err := p.db.View(func(txn *badger.Txn) error {
		firstIndex := p.getFirstIndexInTxn(txn)
		lastIndex := p.getLastIndexInTxn(txn)

		if from < firstIndex {
			panic(IndexOutOfRange(from))
		}
		if to > lastIndex {
			panic(IndexOutOfRange(to))
		}

		for i := from; i <= to; i++ {
			key := p.logEntryKey(i)
			item, err := txn.Get(key)
			if err != nil {
				return err
			}

			err = item.Value(func(val []byte) error {
				entry := &pb.LogEntry{}
				if err := proto.Unmarshal(val, entry); err != nil {
					return err
				}
				entries = append(entries, entry)
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		panic(err)
	}
	return entries
}

func (p *badgerPersistence) FirstEntryIndex() int64 {
	var firstIndex int64 = -1
	err := p.db.View(func(txn *badger.Txn) error {
		firstIndex = p.getFirstIndexInTxn(txn)
		return nil
	})

	if err != nil {
		panic(err)
	}
	return firstIndex
}

func (p *badgerPersistence) LastEntryIndex() int64 {
	var lastIndex int64 = -1
	err := p.db.View(func(txn *badger.Txn) error {
		lastIndex = p.getLastIndexInTxn(txn)
		return nil
	})

	if err != nil {
		panic(err)
	}
	return lastIndex
}

func (p *badgerPersistence) SaveSnapshot(snapshot Snapshot) {
	err := p.db.Update(func(txn *badger.Txn) error {
		metaData, err := proto.Marshal(snapshot.Metadata())
		if err != nil {
			return fmt.Errorf("failed to marshal snapshot metadata: %w", err)
		}

		if err := txn.Set([]byte(snapshotMetaKey), metaData); err != nil {
			return err
		}
		return txn.Set([]byte(snapshotDataKey), snapshot.Data())
	})

	if err != nil {
		panic(err)
	}
}

func (p *badgerPersistence) RetrieveSnapshot() Snapshot {
	var metadata *pb.SnapshotMetadata
	var data []byte
	err := p.db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get([]byte(snapshotMetaKey))
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		}

		err = metaItem.Value(func(val []byte) error {
			metadata = &pb.SnapshotMetadata{}
			return proto.Unmarshal(val, metadata)
		})
		if err != nil {
			return err
		}

		dataItem, err := txn.Get([]byte(snapshotDataKey))
		if err != nil {
			return err
		}

		return dataItem.Value(func(val []byte) error {
			data = make([]byte, len(val))
			copy(data, val)
			return nil
		})
	})

	if err != nil {
		panic(err)
	}

	if metadata == nil {
		return nil
	}

	return NewSnapshot(metadata, data)
}

func (p *badgerPersistence) SnapshotMetadata() *pb.SnapshotMetadata {
	snapshot := p.RetrieveSnapshot()
	if snapshot == nil {
		return nil
	}
	return snapshot.Metadata()
}

func (p *badgerPersistence) Close() {
	if err := p.db.Close(); err != nil {
		fmt.Printf("failed to close badger db: %s\n", err)
	}
}

func (p *badgerPersistence) logEntryKey(index int64) []byte {
	key := make([]byte, len(logEntryPrefix)+8)
	copy(key, logEntryPrefix)
	binary.BigEndian.PutUint64(key[len(logEntryPrefix):], uint64(index))
	return key
}

func (p *badgerPersistence) getFirstIndexInTxn(txn *badger.Txn) int64 {
	item, err := txn.Get([]byte(firstIndexKey))
	if err != nil {
		return -1
	}

	var index int64 = -1
	item.Value(func(val []byte) error {
		if len(val) == 8 {
			index = int64(binary.BigEndian.Uint64(val))
		}
		return nil
	})

	return index
}

func (p *badgerPersistence) getLastIndexInTxn(txn *badger.Txn) int64 {
	item, err := txn.Get([]byte(lastIndexKey))
	if err != nil {
		return -1
	}

	var index int64 = -1
	item.Value(func(val []byte) error {
		if len(val) == 8 {
			index = int64(binary.BigEndian.Uint64(val))
		}
		return nil
	})
	return index
}

func (p *badgerPersistence) setFirstIndexInTxn(txn *badger.Txn, index int64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(index))
	return txn.Set([]byte(firstIndexKey), val)
}

func (p *badgerPersistence) setLastIndexInTxn(txn *badger.Txn, index int64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(index))
	return txn.Set([]byte(lastIndexKey), val)
}
