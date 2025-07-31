package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
)

const (
	logEntryPrefix  = "graft:log:"
	stateKey        = "graft:state"
	snapshotMetaKey = "graft:snapshot:meta"
	snapshotDataKey = "graft:snapshot:data"
	firstIndexKey   = "graft:firstIndex"
	lastIndexKey    = "graft:lastIndex"
)

type badgerPersistence struct {
	db     *badger.DB
	closed bool
}

func OpenBadgerPersistence(dir string) (graft.Persistence, error) {
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

func (p *badgerPersistence) RetrieveState() (*pb.PersistedState, error) {
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
	return state, err
}

func (p *badgerPersistence) SaveState(state *pb.PersistedState) error {
	data, err := proto.Marshal(state)
	if err != nil {
		return err
	}

	return p.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(stateKey), data)
	})
}

func (p *badgerPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	if err := p.SaveState(state); err != nil {
		return 0, err
	}

	if len(entries) == 0 {
		var lastIndex int64 = -1
		if err := p.db.View(func(txn *badger.Txn) error {
			localLastIndex, err := p.getLastIndexInTxn(txn)
			if err != nil {
				return err
			}
			lastIndex = localLastIndex
			return nil
		}); err != nil {
			return 0, err
		}
		return lastIndex, nil
	}

	var nextIndex int64
	err := p.db.Update(func(txn *badger.Txn) error {
		// Get current last index.
		lastIndex, err := p.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
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

	return nextIndex, err
}

func (p *badgerPersistence) TruncateEntriesFrom(index int64) error {
	return p.db.Update(func(txn *badger.Txn) error {
		lastIndex, err := p.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
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

		firstIndex, err := p.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		newLastIndex := index - 1
		if newLastIndex < firstIndex {
			// No entries left
			if err := p.setFirstIndexInTxn(txn, -1); err != nil {
				return err
			}
			return p.setLastIndexInTxn(txn, -1)
		}
		return p.setLastIndexInTxn(txn, newLastIndex)
	})
}

func (p *badgerPersistence) TruncateEntriesTo(index int64) error {
	return p.db.Update(func(txn *badger.Txn) error {
		firstIndex, err := p.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		if firstIndex > index {
			return graft.IndexOutOfRange(index)
		}

		lastIndex, err := p.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
		if lastIndex < index {
			return graft.IndexOutOfRange(index)
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
}

func (p *badgerPersistence) EntryCount() int64 {
	var count int64
	if err := p.db.View(func(txn *badger.Txn) error {
		firstIndex, err := p.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		lastIndex, err := p.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}

		if firstIndex == -1 || lastIndex == -1 {
			count = 0
		} else {
			count = lastIndex - firstIndex + 1
		}

		return nil
	}); err != nil {
		panic(err)
	}
	return count
}

func (p *badgerPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	var entry *pb.LogEntry
	err := p.db.View(func(txn *badger.Txn) error {
		key := p.logEntryKey(index)
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return graft.IndexOutOfRange(index)
			}
			return err
		}

		return item.Value(func(val []byte) error {
			entry = &pb.LogEntry{}
			return proto.Unmarshal(val, entry)
		})
	})
	return entry, err
}

func (p *badgerPersistence) GetEntryTerm(index int64) (int64, error) {
	e, err := p.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (p *badgerPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	if err := p.db.View(func(txn *badger.Txn) error {
		lastIndex, err := p.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
		if index > lastIndex {
			return graft.IndexOutOfRange(index)
		}

		for i := index; i <= lastIndex; i++ {
			key := p.logEntryKey(i)
			item, err := txn.Get(key)
			if err != nil {
				return err
			}

			if err = item.Value(func(val []byte) error {
				entry := &pb.LogEntry{}
				if err := proto.Unmarshal(val, entry); err != nil {
					return err
				}
				entries = append(entries, entry)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func (p *badgerPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	if err := p.db.View(func(txn *badger.Txn) error {
		var err error
		firstIndex, err := p.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		lastIndex, err := p.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}

		if from < firstIndex {
			return graft.IndexOutOfRange(from)
		}
		if to > lastIndex {
			return graft.IndexOutOfRange(to)
		}

		for i := from; i <= to; i++ {
			key := p.logEntryKey(i)
			item, err := txn.Get(key)
			if err != nil {
				return err
			}

			if err := item.Value(func(val []byte) error {
				entry := &pb.LogEntry{}
				if err := proto.Unmarshal(val, entry); err != nil {
					return err
				}
				entries = append(entries, entry)
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func (p *badgerPersistence) FirstEntryIndex() (int64, error) {
	var lastIndex int64 = -1
	if err := p.db.View(func(txn *badger.Txn) error {
		var err error
		lastIndex, err = p.getFirstIndexInTxn(txn)
		return err
	}); err != nil {
		return -1, err
	}
	return lastIndex, nil
}

func (p *badgerPersistence) LastEntryIndex() (int64, error) {
	var lastIndex int64 = -1
	if err := p.db.View(func(txn *badger.Txn) error {
		var err error
		lastIndex, err = p.getLastIndexInTxn(txn)
		return err
	}); err != nil {
		return -1, err
	}
	return lastIndex, nil
}

func (p *badgerPersistence) SaveSnapshot(snapshot graft.Snapshot) error {
	return p.db.Update(func(txn *badger.Txn) error {
		metaData, err := proto.Marshal(snapshot.Metadata())
		if err != nil {
			return err
		}

		if err := txn.Set([]byte(snapshotMetaKey), metaData); err != nil {
			return err
		}
		return txn.Set([]byte(snapshotDataKey), snapshot.Data())
	})
}

func (p *badgerPersistence) RetrieveSnapshot() (graft.Snapshot, error) {
	var metadata *pb.SnapshotMetadata
	var data []byte
	if err := p.db.View(func(txn *badger.Txn) error {
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

		if metadata == nil {
			return nil
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
	}); err != nil {
		return nil, err
	}

	if metadata == nil {
		return nil, nil
	}
	return graft.NewSnapshot(metadata, data), nil
}

func (p *badgerPersistence) SnapshotMetadata() (*pb.SnapshotMetadata, error) {
	var metadata *pb.SnapshotMetadata
	if err := p.db.View(func(txn *badger.Txn) error {
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
		return nil
	}); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (p *badgerPersistence) Close() error {
	return p.db.Close()
}

func (p *badgerPersistence) logEntryKey(index int64) []byte {
	key := make([]byte, len(logEntryPrefix)+8)
	copy(key, logEntryPrefix)
	binary.BigEndian.PutUint64(key[len(logEntryPrefix):], uint64(index))
	return key
}

func (p *badgerPersistence) getFirstIndexInTxn(txn *badger.Txn) (int64, error) {
	item, err := txn.Get([]byte(firstIndexKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return -1, nil
		}
		return -1, err
	}

	var index int64 = -1
	if err := item.Value(func(val []byte) error {
		if len(val) == 8 {
			index = int64(binary.BigEndian.Uint64(val))
		}
		return nil
	}); err != nil {
		return -1, err
	}
	return index, nil
}

func (p *badgerPersistence) getLastIndexInTxn(txn *badger.Txn) (int64, error) {
	item, err := txn.Get([]byte(lastIndexKey))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return -1, nil
		}
		return -1, err
	}

	var index int64 = -1
	if err = item.Value(func(val []byte) error {
		if len(val) == 8 {
			index = int64(binary.BigEndian.Uint64(val))
		}
		return nil
	}); err != nil {
		return -1, err
	}
	return index, nil
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
