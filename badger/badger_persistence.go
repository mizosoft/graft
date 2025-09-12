package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"os"
	"path"
)

const (
	logEntryPrefix      = "graft:log:"
	stateKey            = "graft:state"
	snapshotMetadataKey = "graft:snapshot:meta"
	firstIndexKey       = "graft:firstIndex"
	lastIndexKey        = "graft:lastIndex"
)

type zapLogger struct {
	logger *zap.SugaredLogger
}

func (z *zapLogger) Errorf(s string, i ...interface{}) {
	z.logger.Errorf(logEntryPrefix+s, i...)
}

func (z *zapLogger) Warningf(s string, i ...interface{}) {
	z.logger.Warnf(logEntryPrefix+s, i...)
}

func (z *zapLogger) Infof(s string, i ...interface{}) {
	z.logger.Infof(logEntryPrefix+s, i...)
}

func (z *zapLogger) Debugf(s string, i ...interface{}) {
	z.logger.Debugf(logEntryPrefix+s, i...)
}

type badgerPersistence struct {
	db     *badger.DB
	logger *zap.SugaredLogger
	dir    string
	closed bool
}

func OpenBadgerPersistence(dir string, logger *zap.Logger) (graft.Persistence, error) {
	opts := badger.DefaultOptions(dir)

	var sugaredLogger *zap.SugaredLogger
	if logger != nil {
		sugaredLogger = logger.Sugar()
		opts.Logger = &zapLogger{logger: sugaredLogger}
	}

	opts.SyncWrites = true

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open badger db: %w", err)
	}

	return &badgerPersistence{
		db:     db,
		dir:    dir,
		logger: sugaredLogger,
	}, nil
}

func (b *badgerPersistence) RetrieveState() (*pb.PersistedState, error) {
	var state *pb.PersistedState
	err := b.db.View(func(txn *badger.Txn) error {
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

func (b *badgerPersistence) SaveState(state *pb.PersistedState) error {
	data, err := proto.Marshal(state)
	if err != nil {
		return err
	}
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(stateKey), data)
	})
}

func (b *badgerPersistence) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	if err := b.SaveState(state); err != nil {
		return 0, err
	}

	if len(entries) == 0 {
		var lastIndex int64 = -1
		if err := b.db.View(func(txn *badger.Txn) error {
			localLastIndex, err := b.getLastIndexInTxn(txn)
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
	err := b.db.Update(func(txn *badger.Txn) error {
		// Get current last index.
		lastIndex, err := b.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
		nextIndex = lastIndex + 1

		// Set first index if this is the first entry.
		if lastIndex == -1 {
			if err := b.setFirstIndexInTxn(txn, nextIndex); err != nil {
				return err
			}
		}

		for _, entry := range entries {
			entry.Index = nextIndex
			data, err := proto.Marshal(entry)
			if err != nil {
				return err
			}

			key := b.logEntryKey(nextIndex)
			if err := txn.Set(key, data); err != nil {
				return err
			}
			nextIndex++
		}

		// Update last index.
		return b.setLastIndexInTxn(txn, nextIndex-1)
	})
	return nextIndex, err
}

func (b *badgerPersistence) TruncateEntriesFrom(index int64) error {
	return b.db.Update(func(txn *badger.Txn) error {
		lastIndex, err := b.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
		if lastIndex < index {
			return nil
		}

		// Delete entries from index onwards.
		for i := index; i <= lastIndex; i++ {
			key := b.logEntryKey(i)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		firstIndex, err := b.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		newLastIndex := index - 1
		if newLastIndex < firstIndex {
			// No entries left
			if err := b.setFirstIndexInTxn(txn, -1); err != nil {
				return err
			}
			return b.setLastIndexInTxn(txn, -1)
		}
		return b.setLastIndexInTxn(txn, newLastIndex)
	})
}

func (b *badgerPersistence) TruncateEntriesTo(index int64) error {
	return b.db.Update(func(txn *badger.Txn) error {
		firstIndex, err := b.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		if firstIndex > index {
			return graft.IndexOutOfRange(index)
		}

		lastIndex, err := b.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
		if lastIndex < index {
			return graft.IndexOutOfRange(index)
		}

		// Delete entries up to and including index.
		for i := firstIndex; i <= index; i++ {
			key := b.logEntryKey(i)
			if err := txn.Delete(key); err != nil {
				return err
			}
		}

		newFirstIndex := index + 1
		if newFirstIndex > lastIndex {
			// No entries left
			if err := b.setFirstIndexInTxn(txn, -1); err != nil {
				return err
			}
			return b.setLastIndexInTxn(txn, -1)
		}
		return b.setFirstIndexInTxn(txn, newFirstIndex)
	})
}

func (b *badgerPersistence) EntryCount() int64 {
	var count int64
	if err := b.db.View(func(txn *badger.Txn) error {
		firstIndex, err := b.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		lastIndex, err := b.getLastIndexInTxn(txn)
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

func (b *badgerPersistence) GetEntry(index int64) (*pb.LogEntry, error) {
	var entry *pb.LogEntry
	err := b.db.View(func(txn *badger.Txn) error {
		key := b.logEntryKey(index)
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

func (b *badgerPersistence) GetEntryTerm(index int64) (int64, error) {
	e, err := b.GetEntry(index)
	if err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (b *badgerPersistence) GetEntriesFrom(index int64) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	if err := b.db.View(func(txn *badger.Txn) error {
		lastIndex, err := b.getLastIndexInTxn(txn)
		if err != nil {
			return err
		}
		if index > lastIndex {
			return graft.IndexOutOfRange(index)
		}

		for i := index; i <= lastIndex; i++ {
			key := b.logEntryKey(i)
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

func (b *badgerPersistence) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	if err := b.db.View(func(txn *badger.Txn) error {
		var err error
		firstIndex, err := b.getFirstIndexInTxn(txn)
		if err != nil {
			return err
		}
		lastIndex, err := b.getLastIndexInTxn(txn)
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
			key := b.logEntryKey(i)
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

func (b *badgerPersistence) FirstEntryIndex() (int64, error) {
	var lastIndex int64 = -1
	if err := b.db.View(func(txn *badger.Txn) error {
		var err error
		lastIndex, err = b.getFirstIndexInTxn(txn)
		return err
	}); err != nil {
		return -1, err
	}
	return lastIndex, nil
}

func (b *badgerPersistence) LastEntryIndex() (int64, error) {
	var lastIndex int64 = -1
	if err := b.db.View(func(txn *badger.Txn) error {
		var err error
		lastIndex, err = b.getLastIndexInTxn(txn)
		return err
	}); err != nil {
		return -1, err
	}
	return lastIndex, nil
}

func (b *badgerPersistence) LastSnapshotMetadata() (*pb.SnapshotMetadata, error) {
	var metadata *pb.SnapshotMetadata
	if err := b.db.View(func(txn *badger.Txn) error {
		metaItem, err := txn.Get([]byte(snapshotMetadataKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		} else if err != nil {
			return err
		}

		if err := metaItem.Value(func(val []byte) error {
			metadata = &pb.SnapshotMetadata{}
			return proto.Unmarshal(val, metadata)
		}); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return metadata, nil
}

func (b *badgerPersistence) OpenSnapshot(metadata *pb.SnapshotMetadata) (graft.Snapshot, error) {
	myMetadata, err := b.LastSnapshotMetadata()
	if err != nil {
		return nil, err
	}

	if myMetadata == nil {
		return nil, graft.ErrNoSuchSnapshot
	}
	if metadata.LastAppliedIndex != myMetadata.LastAppliedIndex ||
		metadata.LastAppliedTerm != myMetadata.LastAppliedTerm {
		return nil, graft.ErrNoSuchSnapshot
	}
	return graft.NewFileSnapshot(metadata, path.Join(b.dir, graft.SnapshotFilename(metadata)))
}

type fileSnapshotWriter struct {
	b          *badgerPersistence
	f          *os.File
	metadata   *pb.SnapshotMetadata
	lastOffset int64
	closed     bool
}

func (s *fileSnapshotWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if s.closed {
		return 0, graft.ErrClosed
	}

	if off < 0 || off > s.lastOffset {
		return 0, graft.ErrOffsetOutOfRange
	}
	n, err = s.f.WriteAt(p, off)
	if err == nil {
		s.lastOffset = max(s.lastOffset, off+int64(n))
	}
	return
}

func (s *fileSnapshotWriter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.f.Close(), os.Remove(s.f.Name()))
}

func (s *fileSnapshotWriter) Commit() (*pb.SnapshotMetadata, error) {
	if s.closed {
		return nil, graft.ErrClosed
	}

	s.closed = true

	committed := false
	defer func() {
		if !committed {
			if err := errors.Join(s.f.Close(), os.Remove(s.f.Name())); err != nil {
				s.b.logger.Error("Failed to close and/or delete snapshot file", zap.Error(err))
			}
		}
	}()

	if err := s.f.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(s.f.Name(), path.Join(s.b.dir, graft.SnapshotFilename(s.metadata))); err != nil {
		return nil, err
	}
	if err := s.f.Close(); err != nil {
		return nil, err
	}

	prevMetadata, err := s.b.LastSnapshotMetadata()
	if err != nil {
		return nil, err
	}

	s.metadata.Size = s.lastOffset
	if err := s.b.saveSnapshotMetadata(s.metadata); err != nil {
		return nil, err
	}
	committed = true

	// Delete previous snapshot.
	if prevMetadata != nil {
		if err := os.Remove(path.Join(s.b.dir, graft.SnapshotFilename(prevMetadata))); err != nil {
			s.b.logger.Error("Failed to delete previous snapshot file", zap.Error(err))
		}
	}

	return s.metadata, nil
}

func (b *badgerPersistence) saveSnapshotMetadata(metadata *pb.SnapshotMetadata) error {
	return b.db.Update(func(txn *badger.Txn) error {
		metadataMarshalled, err := proto.Marshal(metadata)
		if err != nil {
			return err
		}

		if err := txn.Set([]byte(snapshotMetadataKey), metadataMarshalled); err != nil {
			return err
		}
		return nil
	})
}

func (b *badgerPersistence) CreateSnapshot(metadata *pb.SnapshotMetadata) (graft.SnapshotWriter, error) {
	fpath := path.Join(b.dir, graft.SnapshotFilename(metadata)+".tmp")
	f, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}
	return &fileSnapshotWriter{b: b, f: f, metadata: metadata}, nil
}

func (b *badgerPersistence) Close() error {
	return b.db.Close()
}

func (b *badgerPersistence) logEntryKey(index int64) []byte {
	key := make([]byte, len(logEntryPrefix)+8)
	copy(key, logEntryPrefix)
	binary.BigEndian.PutUint64(key[len(logEntryPrefix):], uint64(index))
	return key
}

func (b *badgerPersistence) getFirstIndexInTxn(txn *badger.Txn) (int64, error) {
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

func (b *badgerPersistence) getLastIndexInTxn(txn *badger.Txn) (int64, error) {
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

func (b *badgerPersistence) setFirstIndexInTxn(txn *badger.Txn, index int64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(index))
	return txn.Set([]byte(firstIndexKey), val)
}

func (b *badgerPersistence) setLastIndexInTxn(txn *badger.Txn, index int64) error {
	val := make([]byte, 8)
	binary.BigEndian.PutUint64(val, uint64(index))
	return txn.Set([]byte(lastIndexKey), val)
}
