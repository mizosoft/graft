package graft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sort"
)

const (
	headerRecordType = iota
	stateRecordType
	entryRecordType
	snapshotMetadataRecordType
	trailerRecordType
)

const (
	walMagic   uint64 = 0x6420655767271765
	walVersion int32  = 1
)

const (
	recordLengthSize = 4
	minSegmentSize   = 512
)

type entryCache struct {
	data    []*pb.LogEntry
	size    int64
	maxSize int64
}

func (c *entryCache) append(entries []*pb.LogEntry) {
	for _, entry := range entries {
		c.data = append(c.data, entry)
		c.size += int64(len(entry.Data))
	}

	for c.size > c.maxSize {
		e := c.data[0]
		c.data = c.data[1:]
		c.size -= int64(len(e.Data))
	}
}

func (c *entryCache) get(index int64) *pb.LogEntry {
	if index < c.firstIndex() || index > c.lastIndex() {
		return nil
	}
	return c.data[index-c.firstIndex()]
}

func (c *entryCache) getEntries(from, to int64) []*pb.LogEntry {
	if from > to || from < c.firstIndex() || from > c.lastIndex() || to < c.firstIndex() || to > c.lastIndex() {
		return nil
	}
	return c.data[from-c.firstIndex() : to-c.firstIndex()+1]
}

func (c *entryCache) getAll() []*pb.LogEntry {
	return c.data
}

func (c *entryCache) truncateFrom(index int64) {
	if index < c.firstIndex() || index > c.lastIndex() {
		return
	}

	truncated := c.data[index-c.firstIndex():]
	c.data = c.data[:index-c.firstIndex()]
	for _, entry := range truncated {
		c.size -= int64(len(entry.Data))
	}
}

func (c *entryCache) truncateTo(index int64) {
	if index < c.firstIndex() || index > c.lastIndex() {
		return
	}

	truncated := c.data[:index-c.firstIndex()+1]
	c.data = c.data[index-c.firstIndex()+1:]
	for _, entry := range truncated {
		c.size -= int64(len(entry.Data))
	}
}

func (c *entryCache) firstIndex() int64 {
	if len(c.data) == 0 {
		return -1
	}
	return c.data[0].Index
}

func (c *entryCache) lastIndex() int64 {
	if len(c.data) == 0 {
		return -1
	}
	return c.data[len(c.data)-1].Index
}

type wal struct {
	dir                  string
	segmentSize          int64
	segments             []*segment
	crcTable             *crc32.Table
	closed               bool
	trailerRecord        []byte
	memoryMapped         bool
	lastState            *pb.PersistedState
	lastSnapshotMetadata *pb.SnapshotMetadata
	cache                *entryCache
	logger               *zap.SugaredLogger
}

type segment struct {
	w                     *wal
	number                int
	firstIndex, nextIndex int64 // First & next entry indices.
	fpath                 string
	f                     *os.File
	m                     mmap.MMap
	entryOffsets          []int64
	lastOffset            int64
}

func segFileName(number int, firstIndex int64) string {
	return fmt.Sprintf("log_%d_%d.dat", number, firstIndex)
}

func snapshotFilename(metadata *pb.SnapshotMetadata) string {
	return fmt.Sprintf("snap_%d_%d.dat", metadata.LastAppliedIndex, metadata.LastAppliedTerm)
}

func (s *segment) boundedReaderFrom(offset int64) io.Reader {
	if s.m != nil {
		return bytes.NewReader(s.m[offset:s.lastOffset])
	} else {
		return newBufferedReader(io.NewSectionReader(s.f, offset, s.lastOffset-offset))
	}
}

func (s *segment) readerFrom(offset int64) io.Reader {
	if s.m != nil {
		return bytes.NewReader(s.m[offset:])
	} else {
		return newBufferedReader(newReaderAt(s.f, offset))
	}
}

func (s *segment) reader() io.Reader {
	if s.m != nil {
		return bytes.NewReader(s.m)
	} else {
		return newBufferedReader(newReaderAt(s.f, 0))
	}
}

func (s *segment) countingReader() *countingReader {
	return &countingReader{
		r: s.reader(),
	}
}

type countingReader struct {
	r     io.Reader
	count int64
}

func (cr *countingReader) Read(p []byte) (n int, err error) {
	n, err = cr.r.Read(p)
	if n >= 0 {
		cr.count += int64(n)
	}
	if err != nil {
		return n, err
	}
	return n, err
}

func (s *segment) readBytes(offset int64, count int) ([]byte, error) {
	data := make([]byte, count)
	if s.m != nil {
		if copy(data, s.m[offset:]) < count {
			return nil, io.ErrUnexpectedEOF
		}
	} else if _, err := s.f.ReadAt(data, offset); err != nil {
		return nil, err
	}
	return data, nil
}

func (s *segment) appendBytes(data []byte) error {
	if s.m != nil {
		if copy(s.m[s.lastOffset:], data) < len(data) {
			return io.ErrUnexpectedEOF
		}
	} else if _, err := s.f.WriteAt(data, s.lastOffset); err != nil {
		return err
	}
	return nil
}

func (s *segment) sync() error {
	if s.m != nil {
		return s.m.Flush()
	} else {
		return s.f.Sync()
	}
}

func (s *segment) entryCount() int {
	return len(s.entryOffsets)
}

func (s *segment) getEntry(index int64) (*pb.LogEntry, error) {
	localIndex := int(index - s.firstIndex)
	if localIndex < 0 || localIndex >= s.entryCount() {
		return nil, IndexOutOfRange(index)
	}
	return s.getEntryAtOffset(localIndex)
}

func (s *segment) headEntry() (*pb.LogEntry, error) {
	if s.entryCount() == 0 {
		return nil, nil
	}
	return s.getEntryAtOffset(0)
}

func (s *segment) tailEntry() (*pb.LogEntry, error) {
	if s.entryCount() == 0 {
		return nil, nil
	}
	return s.getEntryAtOffset(len(s.entryOffsets) - 1)
}

func (s *segment) getEntryAtOffset(offsetIndex int) (*pb.LogEntry, error) {
	offset := s.entryOffsets[offsetIndex]
	var limit int64
	if offsetIndex < len(s.entryOffsets)-1 {
		limit = s.entryOffsets[offsetIndex+1]
	} else {
		limit = s.lastOffset
	}

	// Note that this chunk might contain multiple records (e.g., saved states/snapshots between the two entries).
	chunk, err := s.readBytes(offset, int(limit-offset))
	if err != nil {
		return nil, err
	}

	record, err := s.w.readRecordFromBytes(chunk)
	if err != nil {
		return nil, err
	}
	if record.Type != entryRecordType {
		return nil, fmt.Errorf("unexpected record type when expecting an entry: %d", record.Type)
	}

	var entry pb.LogEntry
	protoUnmarshal(record.Data, &entry)
	return &entry, nil
}

func (s *segment) getEntriesFrom(from int64) ([]*pb.LogEntry, error) {
	localFrom := int(from - s.firstIndex)
	if localFrom < 0 || localFrom >= s.entryCount() {
		return []*pb.LogEntry{}, nil
	}

	reader := s.readerFrom(s.entryOffsets[localFrom])
	var entries []*pb.LogEntry
	for {
		record, err := s.w.readRecordFromReader(reader)
		if err != nil {
			return nil, err
		}

		switch record.Type {
		case trailerRecordType: // EOF.
			return entries, nil
		case entryRecordType:
			var entry pb.LogEntry
			if err := proto.Unmarshal(record.Data, &entry); err != nil {
				return nil, err
			}
			entries = append(entries, &entry)
		default:
			// skip
		}
	}
}

func (s *segment) getEntriesTo(to int64) ([]*pb.LogEntry, error) {
	localTo := int(to - s.firstIndex)
	if localTo < 0 || localTo >= s.entryCount() {
		return []*pb.LogEntry{}, nil
	}

	reader := s.boundedReaderFrom(s.entryOffsets[0])
	var entries []*pb.LogEntry
	for index := s.firstIndex; index <= to; {
		record, err := s.w.readRecordFromReader(reader)
		if err != nil {
			return nil, err
		}
		if record.Type != entryRecordType {
			continue // Might have some non-entry records.
		}

		var entry pb.LogEntry
		if err := proto.Unmarshal(record.Data, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, &entry)
		index++
	}
	return entries, nil
}

func (s *segment) truncateEntriesFrom(index int64) error {
	localIndex := int(index - s.firstIndex)
	if localIndex < 0 || localIndex >= s.entryCount() {
		return nil // Ignore.
	}

	s.nextIndex = index
	s.lastOffset = s.entryOffsets[localIndex]
	s.entryOffsets = s.entryOffsets[:localIndex]
	return nil
}

func (s *segment) truncateEntriesTo(index int64) (removeHead bool, err error) {
	localIndex := int(index - s.firstIndex)
	if localIndex < 0 || localIndex >= s.entryCount() {
		return false, nil // Ignore.
	}

	// If all entries are to be truncated then delete this segment.
	if localIndex >= len(s.entryOffsets)-1 {
		return true, s.delete()
	}

	// Write the new segment into a temp file and do an atomic replace.
	newFirstIndex := index + 1
	newFpath := path.Join(s.w.dir, segFileName(s.number, newFirstIndex))
	tempNewFpath := newFpath + ".tmp"
	tempF, err := os.Create(tempNewFpath)
	if err != nil {
		return false, err
	}
	if err := tempF.Truncate(s.w.segmentSize); err != nil {
		return false, err
	}

	var newLastOffset int64
	var offsetDiff int64
	err = func() error {
		// Append a new header record.
		buf := s.w.appendRecordTo(nil, headerRecordType, &pb.WalSegmentHeader{
			Magic:         walMagic,
			Version:       walVersion,
			Flags:         0,
			SegmentNumber: int32(s.number),
			FirstIndex:    newFirstIndex,
		})
		nHeader, err := tempF.WriteAt(buf, 0)
		if err != nil {
			return err
		}

		// Copy remaining records.
		copyFromOffset := s.entryOffsets[localIndex+1]
		copyToOffset := s.lastOffset + int64(len(s.w.trailerRecord)) // Include the trailer record when copying.
		nCopied, err := io.Copy(newWriterAt(tempF, int64(nHeader)), io.NewSectionReader(s.f, copyFromOffset, copyToOffset-copyFromOffset))
		if err != nil {
			return err
		}
		if err := tempF.Sync(); err != nil {
			return err
		}
		if err := tempF.Close(); err != nil {
			return err
		}

		// We want oldOffset + offsetDiff = newOffset.
		offsetDiff = int64(len(buf)) - copyFromOffset
		newLastOffset = int64(len(buf)) + nCopied - int64(len(s.w.trailerRecord))
		return nil
	}()
	if err != nil {
		return false, removeOnErr(tempNewFpath, closeOnErr(tempF, err))
	}

	// Close, do atomic replace, re-open.

	if err := s.close(); err != nil {
		return false, removeOnErr(tempNewFpath, closeOnErr(tempF, err))
	}

	if err := os.Rename(tempNewFpath, newFpath); err != nil {
		return false, removeOnErr(tempNewFpath, closeOnErr(tempF, err))
	}

	s.entryOffsets = s.entryOffsets[localIndex+1:]

	// Shift the offsets by the amount we deleted.
	for i := range s.entryOffsets {
		s.entryOffsets[i] += offsetDiff
	}

	newF, err := os.OpenFile(newFpath, os.O_RDWR, 0644)
	if err != nil {
		return false, err
	}

	var newM []byte
	if s.w.memoryMapped {
		newM, err = mmap.Map(newF, mmap.RDWR, 0)
		if err != nil {
			return false, closeOnErr(newF, err)
		}
	}

	s.firstIndex = newFirstIndex
	s.m, s.f, s.fpath = newM, newF, newFpath
	s.lastOffset = newLastOffset
	return false, nil
}

func (s *segment) delete() error {
	if err := s.close(); err != nil {
		return err
	}
	if err := os.Remove(s.fpath); err != nil {
		return err
	}
	return nil
}

func (s *segment) close() error {
	var err error
	if s.m != nil {
		err = s.m.Unmap()
	}
	if s.f != nil {
		if err2 := s.f.Close(); err2 != nil {
			err = errors.Join(err, err2)
		}
	}
	return err
}

type WalOptions struct {
	Dir             string
	SegmentSize     int64
	SuffixCacheSize int64
	MemoryMapped    bool
	Logger          *zap.Logger
}

func (o *WalOptions) Validate() error {
	if o.Dir == "" {
		return errors.New("dir is required")
	}
	if o.SegmentSize < minSegmentSize {
		return errors.New("segment size is too low")
	}
	if o.SuffixCacheSize < 0 {
		return errors.New("suffix cache size must be positive")
	}
	return nil
}

func OpenWal(options WalOptions) (Persistence, error) {
	return openWal(options)
}

func openWal(options WalOptions) (*wal, error) {
	if err := options.Validate(); err != nil {
		return nil, err
	}

	files, err := os.ReadDir(options.Dir)
	if err != nil {
		return nil, err
	}

	if options.Logger == nil {
		options.Logger = zap.NewNop()
	}

	w := &wal{
		dir:         options.Dir,
		segmentSize: options.SegmentSize,
		segments:    make([]*segment, 0),
		crcTable:    crc32.MakeTable(crc32.Castagnoli),
		cache: &entryCache{
			data:    make([]*pb.LogEntry, 0),
			maxSize: options.SuffixCacheSize,
		},
		memoryMapped: options.MemoryMapped,
		logger:       options.Logger.With(zap.String("name", "WAL")).Sugar(),
	}

	w.trailerRecord = w.appendRecordTo(nil, trailerRecordType, &pb.WalSegmentTrailer{Magic: walMagic})

	// Find segments.
	var segments []*segment

	allGood := false
	defer func() {
		if !allGood {
			for _, s := range segments {
				if err := s.close(); err != nil {
					w.logger.Warnf("Error closing segment %s: %v", s.fpath, err)
				}
			}
		}
	}()

	for _, file := range files {
		if file.IsDir() {
			w.logger.Warnf("Warning: unexpected directory found in WAL directory: %s", file.Name())
			continue
		}

		var segNum int
		var firstIndex int64
		if n, err := fmt.Sscanf(file.Name(), "log_%d_%d.dat", &segNum, &firstIndex); err != nil || n != 2 {
			w.logger.Warnf("Warning: unexpected file in WAL directory: %s", file.Name())
			continue
		}

		s := &segment{
			w:            w,
			number:       segNum,
			firstIndex:   firstIndex,
			nextIndex:    firstIndex,
			fpath:        path.Join(options.Dir, file.Name()),
			entryOffsets: []int64{},
		}
		w.segments = append(w.segments, s) // Append early so that the file / memory-mapping is cleaned if an err occurs.

		s.f, err = os.OpenFile(s.fpath, os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		if s.w.memoryMapped {
			s.m, err = mmap.Map(s.f, mmap.RDWR, 0)
			if err != nil {
				return nil, err // We don't need closeOnErr here as the allGood deferred hook will close the file.
			}
		}

		reader := s.countingReader()

		// Expect header record at the beginning.
		record, err := w.readRecordFromReader(reader)
		if err != nil {
			return nil, err
		}
		if record.Type != headerRecordType {
			return nil, fmt.Errorf("unexpected record type: %d", record.Type)
		}

		var header pb.WalSegmentHeader
		if err := proto.Unmarshal(record.Data, &header); err != nil {
			return nil, err
		}
		if header.Magic != walMagic {
			return nil, fmt.Errorf("invalid WAL header magic number in %s", s.fpath)
		}
		if header.Version != walVersion {
			return nil, fmt.Errorf("unsupported WAL version in %s", s.fpath)
		}

		correctedSegmentNumber := s.number
		correctedFirstIndex := s.firstIndex
		if int(header.SegmentNumber) != s.number {
			w.logger.Warnf(
				"Warning: segment number from file name (%s) disagrees with header's (%d), believing the latter",
				s.fpath, header.SegmentNumber)
			correctedSegmentNumber = int(header.SegmentNumber)
		}
		if header.FirstIndex != s.firstIndex {
			w.logger.Warnf(
				"Warning: first index from file name (%s) disagrees with header's (%d), believing the latter",
				s.fpath, header.FirstIndex)
			correctedFirstIndex = header.FirstIndex
		}

		if correctedSegmentNumber != s.number || correctedFirstIndex != s.firstIndex {
			correctedFpath := path.Join(w.dir, segFileName(correctedSegmentNumber, correctedFirstIndex))
			if err := os.Rename(s.fpath, correctedFpath); err != nil {
				return nil, err
			}

			s.number, s.firstIndex = correctedSegmentNumber, correctedFirstIndex
			s.fpath = correctedFpath
		}

	outerLoop:
		for {
			record, err := w.readRecordFromReader(reader)
			if err != nil {
				return nil, err
			}

			switch record.Type {
			case entryRecordType:
				s.entryOffsets = append(s.entryOffsets, s.lastOffset)
				s.nextIndex++

				var entry pb.LogEntry
				protoUnmarshal(record.Data, &entry)
				w.cache.append([]*pb.LogEntry{&entry})
			case stateRecordType:
				var state pb.PersistedState
				if err := proto.Unmarshal(record.Data, &state); err != nil {
					return nil, err
				}
				w.lastState = &state
			case snapshotMetadataRecordType:
				var snapshot pb.SnapshotMetadata
				protoUnmarshal(record.Data, &snapshot)
				w.lastSnapshotMetadata = &snapshot
			case trailerRecordType:
				var trailer pb.WalSegmentTrailer
				protoUnmarshal(record.Data, &trailer)
				if trailer.Magic != walMagic {
					return nil, fmt.Errorf("invalid WAL trailer magic number in %s", s.fpath)
				}
				s.lastOffset = reader.count - int64(len(w.trailerRecord))
				break outerLoop
			case headerRecordType:
				return nil, fmt.Errorf("unexpected header record found in segment %s", s.fpath)
			default:
				return nil, fmt.Errorf("unexpected record type: %d", record.Type)
			}
		}
	}

	// Sort segment files.
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].number < segments[j].number
	})

	// Verify index continuity.
	for i, seg := range w.segments {
		if i > 0 {
			prev := w.segments[i-1]
			if prev.firstIndex > seg.firstIndex {
				return nil, fmt.Errorf("firstIndex is not ordered: %d (%d) > %d (%d)",
					prev.firstIndex, prev.number, seg.firstIndex, seg.number)
			}
			if prev.nextIndex != seg.firstIndex {
				return nil, fmt.Errorf("gap detected between segments %s (ends at %d) and %s (starts at %d)",
					prev.fpath, prev.nextIndex, seg.fpath, seg.firstIndex)
			}
		}
	}

	if len(w.segments) == 0 {
		if err := w.appendSegment(); err != nil {
			return nil, err
		}
	}
	allGood = true
	return w, nil
}

func (w *wal) verify(record *pb.WalRecord) bool {
	return w.crc32Of(record.Data) == record.Crc32
}

func (w *wal) decodeRecord(recordBytes []byte) (*pb.WalRecord, error) {
	var record pb.WalRecord
	if err := proto.Unmarshal(recordBytes, &record); err != nil {
		return nil, err
	}
	if w.verify(&record) {
		return &record, nil
	}
	return nil, ErrCorrupt
}

func (w *wal) RetrieveState() (*pb.PersistedState, error) {
	return w.lastState, nil
}

func (w *wal) saveState(state *pb.PersistedState) error {
	if err := w.appendRecord(stateRecordType, state); err != nil {
		return err
	}
	w.lastState = state
	return nil
}

func (w *wal) SaveState(state *pb.PersistedState) error {
	if w.closed {
		return ErrClosed
	}

	if proto.Equal(state, w.lastState) {
		return nil
	}
	return w.saveState(state)
}

func (w *wal) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	if w.closed {
		return 0, ErrClosed
	}

	if proto.Equal(state, w.lastState) {
		state = nil // Don't append.
	}

	nextIndex := w.tail().nextIndex

	if state == nil && len(entries) == 0 {
		return nextIndex, nil
	}

	if len(entries) == 0 {
		return nextIndex, w.SaveState(state)
	}

	for _, entry := range entries {
		entry.Index = nextIndex
		nextIndex++
	}

	// Append as many entries as possible to each segment.
	var buf bytes.Buffer
	var offsets []int
	for _, entry := range entries {
		offsets = append(offsets, buf.Len())
		w.appendRecordToBuffer(&buf, entryRecordType, entry)

		if w.tail().lastOffset+int64(buf.Len()) > w.segmentSize-int64(len(w.trailerRecord)) {
			// We'll exceed the current segment's capacity by appending up to this entry. Append prior entries to current segment
			// then create a new one.
			if len(offsets) > 1 {
				writtenCount := offsets[len(offsets)-1]
				data := append([]byte(nil), buf.Next(writtenCount)...) // Do a copy.
				if err := w.tail().appendBytes(append(data, w.trailerRecord...)); err != nil {
					return 0, err
				}
				for _, offset := range offsets[:len(offsets)-1] {
					w.tail().entryOffsets = append(w.tail().entryOffsets, w.tail().lastOffset+int64(offset))
				}
				w.tail().lastOffset += int64(writtenCount)
				w.tail().nextIndex += int64(len(offsets) - 1)
				offsets[0] = 0
				offsets = offsets[:1]
			}

			if err := w.tail().sync(); err != nil {
				return 0, err
			}
			if err := w.appendSegment(); err != nil {
				return 0, err
			}
		}
		if w.tail().lastOffset+int64(buf.Len()) > w.segmentSize-int64(len(w.trailerRecord)) {
			return 0, ErrLargeRecord
		}
	}

	// Here, we write whatever is in the buffer since we're sure the last segment's capacity suffices (according the prev
	// loop's invariant).
	if len(offsets) > 0 {
		writtenCount := buf.Len()
		if err := w.tail().appendBytes(append(buf.Bytes(), w.trailerRecord...)); err != nil {
			return 0, err
		}
		for _, offset := range offsets {
			w.tail().entryOffsets = append(w.tail().entryOffsets, w.tail().lastOffset+int64(offset))
		}
		w.tail().lastOffset += int64(writtenCount)
		w.tail().nextIndex += int64(len(offsets))
	}

	if state != nil {
		if err := w.saveState(state); err != nil { // This will do a sync.
			return 0, err
		}
	} else if err := w.tail().sync(); err != nil {
		return 0, err
	}

	w.cache.append(cloneMsgs(entries)) // Defensively copy before caching.
	return nextIndex, nil
}

func (w *wal) appendSegment() error {
	firstIndex := int64(0)
	segNumber := 0
	if len(w.segments) > 0 {
		firstIndex = w.tail().nextIndex
		segNumber = w.tail().number + 1
	}

	s := &segment{
		w:            w,
		number:       segNumber,
		firstIndex:   firstIndex,
		nextIndex:    firstIndex,
		fpath:        path.Join(w.dir, segFileName(segNumber, firstIndex)),
		entryOffsets: []int64{},
	}

	var buf bytes.Buffer
	w.appendRecordToBuffer(&buf, headerRecordType, &pb.WalSegmentHeader{
		Magic:         walMagic,
		Version:       walVersion,
		Flags:         0,
		SegmentNumber: int32(s.number),
		FirstIndex:    firstIndex,
	})

	// Make sure each segment knows the last state & snapshot.
	if w.lastState != nil {
		w.appendRecordToBuffer(&buf, stateRecordType, w.lastState)
	}
	if w.lastSnapshotMetadata != nil {
		w.appendRecordToBuffer(&buf, snapshotMetadataRecordType, w.lastSnapshotMetadata)
	}

	writtenCount := buf.Len()
	if int64(writtenCount) > w.segmentSize-int64(len(w.trailerRecord)) {
		return ErrLargeRecord
	}

	tempFpath := s.fpath + ".tmp"
	tempF, err := os.Create(tempFpath)
	if err != nil {
		return err
	}
	err = func() error {
		if err := tempF.Truncate(w.segmentSize); err != nil {
			return err
		}

		buf.Write(w.trailerRecord)
		if _, err := buf.WriteTo(tempF); err != nil {
			return err
		}
		if err := tempF.Sync(); err != nil {
			return err
		}
		if err := tempF.Close(); err != nil {
			return err
		}
		return nil
	}()
	if err != nil {
		return removeOnErr(tempFpath, closeOnErr(tempF, err))
	}

	if err := os.Rename(tempFpath, s.fpath); err != nil {
		return err
	}

	f, err := os.OpenFile(s.fpath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	var m []byte
	if s.w.memoryMapped {
		m, err = mmap.Map(f, mmap.RDWR, 0)
		if err != nil {
			return closeOnErr(f, err)
		}
	}

	s.f, s.m = f, m
	s.lastOffset = int64(writtenCount)
	w.segments = append(w.segments, s)
	return nil
}

func (w *wal) TruncateEntriesFrom(index int64) error {
	if w.closed {
		return ErrClosed
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		return err
	}

	removedSegments := w.segments[segIndex+1:]
	for _, seg := range removedSegments {
		if err := seg.delete(); err != nil {
			return err
		}
	}
	if err := w.segments[segIndex].truncateEntriesFrom(index); err != nil {
		return err
	}

	w.segments = w.segments[0 : segIndex+1]
	if w.cache.firstIndex() >= 0 {
		w.cache.truncateFrom(max(index, w.cache.firstIndex()))
	}

	// Save last state & snapshot metadata since they could've been truncated.
	if w.lastState != nil {
		if err := w.saveState(w.lastState); err != nil {
			return err
		}
	}
	if w.lastSnapshotMetadata != nil {
		if err := w.saveSnapshotMetadata(w.lastSnapshotMetadata); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) TruncateEntriesTo(index int64) error {
	if w.closed {
		return ErrClosed
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		return err
	}

	removedSegments := w.segments[:segIndex]
	for _, seg := range removedSegments {
		if err := seg.delete(); err != nil {
			return err
		}
	}

	removeHead, err := w.segments[segIndex].truncateEntriesTo(index)
	if err != nil {
		return err
	}

	w.segments = w.segments[segIndex:]
	if removeHead { // Head segment is deleted.
		if len(w.segments) == 1 {
			if err := w.appendSegment(); err != nil {
				return err
			}
		}
		w.segments = w.segments[1:]
	}
	if w.cache.firstIndex() >= 0 && index >= w.cache.firstIndex() {
		w.cache.truncateTo(index)
	}

	// Save last state & snapshot metadata since they could've been truncated.
	if w.lastState != nil {
		if err := w.saveState(w.lastState); err != nil {
			return err
		}
	}
	if w.lastSnapshotMetadata != nil {
		if err := w.saveSnapshotMetadata(w.lastSnapshotMetadata); err != nil {
			return err
		}
	}
	return nil
}

func (w *wal) EntryCount() int64 {
	count := int64(0)
	for _, seg := range w.segments {
		count += int64(seg.entryCount())
	}
	return count
}

func (w *wal) GetEntry(index int64) (*pb.LogEntry, error) {
	if w.closed {
		return nil, ErrClosed
	}

	e := w.cache.get(index)
	if e != nil {
		return e, nil
	} else {
		return w.getEntry(index)
	}
}

func (w *wal) getEntry(index int64) (*pb.LogEntry, error) {
	segIndex, err := w.findSegment(index)
	if err != nil {
		return nil, err
	}

	entry, err := w.segments[segIndex].getEntry(index)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

func (w *wal) GetEntryTerm(index int64) (int64, error) {
	e, err := w.GetEntry(index)
	if err != nil {
		return 0, err
	} else {
		return e.Term, nil
	}
}

func (w *wal) GetEntries(from, to int64) ([]*pb.LogEntry, error) {
	if from > to {
		return nil, fmt.Errorf("from (%d) must be smaller than or equal to (%d)", from, to)
	}

	if w.cache.firstIndex() >= 0 && to >= w.cache.firstIndex() {
		entries := w.cache.getEntries(max(w.cache.firstIndex(), from), to)
		if from < w.cache.firstIndex() {
			suffix := entries
			prefix, err := w.getEntries(from, w.cache.firstIndex()-1)
			if err != nil {
				return nil, err
			}
			entries = append(prefix, suffix...)
		}
		return entries, nil
	} else {
		return w.getEntries(from, to)
	}
}

func (w *wal) getEntries(from, to int64) ([]*pb.LogEntry, error) {
	if from == to {
		e, err := w.getEntry(from)
		if err != nil {
			return nil, err
		} else {
			return []*pb.LogEntry{e}, nil
		}
	}

	firstSegIndex, err := w.findSegment(from)
	if err != nil {
		return nil, err
	}

	lastSegIndex, err := w.findSegment(to)
	if err != nil {
		return nil, err
	}

	entries, err := w.segments[firstSegIndex].getEntriesFrom(from)
	if err != nil {
		return nil, err
	}

	if firstSegIndex == lastSegIndex {
		return entries[0 : to-from+1], nil
	} else {
		for i := firstSegIndex + 1; i < lastSegIndex; i++ {
			seg := w.segments[i]
			middleEntries, err := seg.getEntriesFrom(seg.firstIndex)
			if err != nil {
				return nil, err
			}
			entries = append(entries, middleEntries...)
		}

		tailEntries, err := w.segments[lastSegIndex].getEntriesTo(to)
		if err != nil {
			return nil, err
		}
		return append(entries, tailEntries...), nil
	}
}

func (w *wal) HeadEntry() (*pb.LogEntry, error) {
	if w.closed {
		return nil, ErrClosed
	}

	// Search for the first segment from the start that has an entry.
	for i := 0; i < len(w.segments); i++ {
		seg := w.segments[i]
		entry, err := seg.headEntry()
		if err != nil {
			return nil, err
		}
		if entry != nil {
			return entry, nil
		}
	}
	return nil, nil
}

func (w *wal) TailEntry() (*pb.LogEntry, error) {
	if w.closed {
		return nil, ErrClosed
	}

	// Search for the first segment from the end that has an entry.
	for i := len(w.segments) - 1; i >= 0; i-- {
		seg := w.segments[i]
		entry, err := seg.tailEntry()
		if err != nil {
			return nil, err
		}
		if entry != nil {
			return entry, err
		}
	}
	return nil, nil
}

func (w *wal) GetEntriesFrom(from int64) ([]*pb.LogEntry, error) {
	if w.closed {
		return nil, ErrClosed
	}

	if w.cache.firstIndex() >= 0 && from >= w.cache.firstIndex() {
		return w.cache.getEntries(from, w.cache.lastIndex()), nil
	} else if w.cache.firstIndex() >= 0 {
		entries, err := w.getEntries(from, w.cache.firstIndex()-1)
		if err != nil {
			return nil, err
		}
		entries = append(entries, w.cache.getAll()...)
		return entries, nil
	} else {
		return w.getEntriesFrom(from)
	}
}

func (w *wal) getEntriesFrom(from int64) ([]*pb.LogEntry, error) {
	segIndex, err := w.findSegment(from)
	if err != nil {
		return nil, err
	}

	entries, err := w.segments[segIndex].getEntriesFrom(from)
	if err != nil {
		return nil, err
	}

	for i := segIndex + 1; i < len(w.segments); i++ {
		seg := w.segments[i]
		segEntries, err := seg.getEntriesFrom(seg.firstIndex)
		if err != nil {
			return nil, err
		}
		entries = append(entries, segEntries...)
	}
	return entries, nil
}

func (w *wal) saveSnapshotMetadata(metadata *pb.SnapshotMetadata) error {
	if err := w.appendRecord(snapshotMetadataRecordType, metadata); err != nil {
		return err
	}
	w.lastSnapshotMetadata = metadata
	return nil
}

func (w *wal) FirstEntryIndex() (int64, error) {
	for _, seg := range w.segments {
		if seg.entryCount() > 0 {
			return seg.firstIndex, nil
		}
	}
	return -1, nil
}

func (w *wal) LastEntryIndex() (int64, error) {
	for i := len(w.segments) - 1; i >= 0; i-- {
		seg := w.segments[i]
		if seg.entryCount() > 0 {
			return seg.nextIndex - 1, nil
		}
	}
	return -1, nil
}

func (w *wal) Close() error {
	if w.closed {
		return ErrClosed
	}

	var errs []error
	for _, s := range w.segments {
		if err := s.close(); err != nil {
			errs = append(errs, err)
		}
	}
	w.closed = true

	if len(errs) > 0 {
		w.logger.Error("Close errors", zap.Errors("errors", errs))
	}
	return nil
}

func (w *wal) LastSnapshotMetadata() (*pb.SnapshotMetadata, error) {
	if w.closed {
		return nil, ErrClosed
	}
	return w.lastSnapshotMetadata, nil
}

func (w *wal) OpenSnapshot(metadata *pb.SnapshotMetadata) (Snapshot, error) {
	if w.closed {
		return nil, ErrClosed
	}

	myMetadata := w.lastSnapshotMetadata
	if myMetadata == nil {
		return nil, ErrNoSuchSnapshot
	}
	if metadata.LastAppliedIndex != myMetadata.LastAppliedIndex ||
		metadata.LastAppliedTerm != myMetadata.LastAppliedTerm {
		return nil, ErrNoSuchSnapshot
	}

	data, err := os.ReadFile(path.Join(w.dir, snapshotFilename(metadata)))
	if err != nil {
		return nil, err
	}
	return NewSnapshot(metadata, data), nil
}

func (w *wal) NewSnapshot(metadata *pb.SnapshotMetadata) (SnapshotWriter, error) {
	if w.closed {
		return nil, ErrClosed
	}

	fpath := path.Join(w.dir, snapshotFilename(metadata)+".tmp")
	f, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}
	return &diskSnapshotWriter{w: w, metadata: metadata, f: f}, nil
}

// May want to return "committed".
type diskSnapshotWriter struct {
	w          *wal
	metadata   *pb.SnapshotMetadata
	f          *os.File
	lastOffset int64
	closed     bool
}

func (d *diskSnapshotWriter) WriteAt(p []byte, off int64) (n int, err error) {
	if d.closed {
		return 0, ErrClosed
	}

	if off < 0 || off > d.lastOffset {
		return 0, ErrOffsetOutOfRange
	}
	n, err = d.f.WriteAt(p, off)
	if err == nil {
		d.lastOffset = max(d.lastOffset, off+int64(n))
	}
	return
}

func (d *diskSnapshotWriter) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	return errors.Join(d.f.Close(), os.Remove(d.f.Name()))
}

func (d *diskSnapshotWriter) Commit() (*pb.SnapshotMetadata, error) {
	if d.closed {
		return nil, ErrClosed
	}

	d.closed = true

	committed := false
	defer func() {
		if !committed {
			if err := errors.Join(d.f.Close(), os.Remove(d.f.Name())); err != nil {
				d.w.logger.Error("Failed to close/delete snapshot file", zap.Error(err))
			}
		}
	}()

	if err := d.f.Sync(); err != nil {
		return nil, err
	}
	if err := os.Rename(d.f.Name(), path.Join(d.w.dir, snapshotFilename(d.metadata))); err != nil {
		return nil, err
	}
	if err := d.f.Close(); err != nil {
		return nil, err
	}

	d.metadata.Size = d.lastOffset
	if err := d.w.saveSnapshotMetadata(d.metadata); err != nil {
		return nil, err
	}
	committed = true
	return d.metadata, nil
}

func (w *wal) findSegment(entryIndex int64) (int, error) {
	lo, hi := 0, len(w.segments)
	for lo < hi {
		mid := (lo + hi) / 2
		if entryIndex >= w.segments[mid].nextIndex {
			lo = mid + 1
		} else if entryIndex < w.segments[mid].firstIndex {
			hi = mid
		} else {
			return mid, nil
		}
	}
	return -1, IndexOutOfRange(entryIndex)
}

func (w *wal) crc32Of(data []byte) uint32 {
	hash := crc32.New(w.crcTable)
	hash.Write(data)
	return hash.Sum32()
}

func (w *wal) appendRecordTo(buf []byte, recordType uint32, msg proto.Message) []byte {
	encodedMsg := protoMarshal(msg)
	recordBytes := protoMarshal(&pb.WalRecord{Type: recordType, Crc32: w.crc32Of(encodedMsg), Data: encodedMsg})
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(recordBytes)))
	buf = append(buf, recordBytes...)
	return buf
}

func (w *wal) appendRecordToBuffer(buf *bytes.Buffer, recordType uint32, msg proto.Message) {
	encodedMsg := protoMarshal(msg)
	recordBytes := protoMarshal(&pb.WalRecord{Type: recordType, Crc32: w.crc32Of(encodedMsg), Data: encodedMsg})
	binary.Write(buf, binary.BigEndian, uint32(len(recordBytes)))
	buf.Write(recordBytes)
}

func (w *wal) appendRecord(recordType uint32, msg proto.Message) error {
	recordBytes := w.appendRecordTo(nil, recordType, msg)
	if w.tail().lastOffset+int64(len(recordBytes)) > w.segmentSize-int64(len(w.trailerRecord)) {
		if err := w.appendSegment(); err != nil {
			return err
		}
	}
	if w.tail().lastOffset+int64(len(recordBytes)) > w.segmentSize-int64(len(w.trailerRecord)) {
		return ErrLargeRecord
	}
	if err := w.tail().appendBytes(append(recordBytes, w.trailerRecord...)); err != nil {
		return err
	}
	if err := w.tail().sync(); err != nil {
		return err
	}
	w.tail().lastOffset += int64(len(recordBytes))
	return nil
}

func (w *wal) readRecordFromBytes(chunk []byte) (*pb.WalRecord, error) {
	recordLen := int(binary.BigEndian.Uint32(chunk))
	if recordLen <= 0 || int64(recordLen) > w.segmentSize {
		return nil, ErrCorrupt
	}

	record, err := w.decodeRecord(chunk[recordLengthSize : recordLengthSize+recordLen])
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (w *wal) readRecordFromReader(reader io.Reader) (*pb.WalRecord, error) {
	var recordLen int32
	if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
		return nil, err
	}
	if recordLen <= 0 || int64(recordLen) > w.segmentSize {
		return nil, ErrCorrupt
	}

	recordBytes := make([]byte, recordLen)
	if n, err := reader.Read(recordBytes); err != nil && n < len(recordBytes) {
		return nil, err
	}

	record, err := w.decodeRecord(recordBytes)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (w *wal) tail() *segment {
	return w.segments[len(w.segments)-1]
}
