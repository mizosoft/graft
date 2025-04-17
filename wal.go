package graft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path"
	"sort"

	"github.com/mizosoft/graft/pb"
	"google.golang.org/protobuf/proto"
)

const (
	StateType = iota
	EntryType
	HeaderType
)

const (
	walMagic   uint64 = 0x6420655767271765
	walVersion int32  = 1
)

var (
	errClosed = errors.New("closed")
)

type wal struct {
	dir             string
	softSegmentSize int64
	segments        []*segment
	tail            *segment
	crcTable        *crc32.Table
	closed          bool
	lastState       *pb.PersistedState // Last-written state.
	lastLogTerm     int
}

type segment struct {
	w                     *wal
	number                int
	firstIndex, nextIndex int // First & next entry indices.
	fname                 string
	f                     *os.File
	entryOffsets          []int64
	lastOffset            int64
}

func (s *segment) entryCount() int {
	return len(s.entryOffsets)
}

func (s *segment) getEntry(index int) (*pb.LogEntry, error) {
	localIndex := index - s.firstIndex
	if localIndex < 0 || localIndex >= s.entryCount() {
		return nil, indexOutOfRange(index)
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

	chunk := make([]byte, limit-offset)
	if n, err := s.f.ReadAt(chunk, offset); err != nil && n < len(chunk) {
		return nil, err
	}

	recordLen := int(binary.BigEndian.Uint32(chunk))
	record, err := s.w.decodeRecord(chunk[4 : 4+recordLen])
	if err != nil {
		return nil, err
	}
	if record.Type != EntryType {
		panic(fmt.Errorf("unexpected record type when expecting an entry: %d", record.Type))
	}

	var entry pb.LogEntry
	if err := proto.Unmarshal(record.Data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (s *segment) getEntriesFrom(index int) ([]*pb.LogEntry, error) {
	localIndex := index - s.firstIndex
	if localIndex < 0 || localIndex >= s.entryCount() {
		return []*pb.LogEntry{}, nil
	}

	reader := newBufferedReader(newReaderAt(s.f, s.entryOffsets[localIndex]))
	for entries := []*pb.LogEntry{}; ; {
		var recordLen int32
		if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
			if errors.Is(err, io.EOF) {
				return entries, nil
			}
			return nil, err
		}

		recordBytes := make([]byte, recordLen)
		if n, err := reader.Read(recordBytes); err != nil && n < len(recordBytes) {
			return nil, err
		}

		record, err := s.w.decodeRecord(recordBytes)
		if err != nil {
			return nil, err
		}
		if record.Type != EntryType {
			continue // Might have some non-entry records.
		}

		var entry pb.LogEntry
		if err := proto.Unmarshal(record.Data, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, &entry)
	}
}

func (s *segment) truncateEntriesFrom(index int) error {
	localIndex := index - s.firstIndex
	if localIndex < 0 || localIndex >= s.entryCount() {
		return nil // Ignore.
	}

	offset := s.entryOffsets[localIndex]
	if err := s.f.Truncate(offset); err != nil {
		return err
	}
	if _, err := s.f.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	s.nextIndex = index
	s.entryOffsets = s.entryOffsets[:localIndex]
	s.lastOffset = offset

	// Re-append the latest state after truncation.
	if s.w.lastState != nil {
		if err := s.appendState(s.w.lastState); err != nil {
			return err
		}
	} else if err := s.f.Sync(); err != nil {
		return err
	}
	return nil
}

func (s *segment) delete() error {
	if err := s.f.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.fname); err != nil {
		return err
	}
	return nil
}

func (s *segment) appendState(state *pb.PersistedState) error {
	record, err := s.w.recordOf(StateType, state)
	if err != nil {
		return err
	}
	recordBytes, err := proto.Marshal(record)
	if err != nil {
		return err
	}

	byteCount := 4 + len(recordBytes)
	chunk := make([]byte, byteCount)
	binary.BigEndian.PutUint32(chunk, uint32(len(recordBytes)))
	copy(chunk[4:], recordBytes)
	if _, err := s.f.Write(chunk); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.lastOffset += int64(byteCount)
	return nil
}

func (s *segment) append(state *pb.PersistedState, entries []*pb.LogEntry) (int, error) {
	if state == nil && len(entries) == 0 {
		return s.nextIndex, nil
	}

	if len(entries) == 0 {
		return s.nextIndex, s.appendState(state)
	} else if int(entries[0].Index) != s.nextIndex {
		return -1, fmt.Errorf(
			"appending entries with mismatching index continuity, expected %d, got %d", s.nextIndex, entries[0].Term)
	}

	records := make([]*pb.Record, 0, 1+len(entries))
	for _, entry := range entries {
		record, err := s.w.recordOf(EntryType, entry)
		if err != nil {
			return -1, err
		}
		records = append(records, record)
	}

	if state != nil {
		record, err := s.w.recordOf(StateType, state)
		if err != nil {
			return -1, err
		}
		records = append(records, record)
	}

	lastOffset := s.lastOffset
	var offsets []int64
	var buf bytes.Buffer
	for _, record := range records {
		recordBytes, err := proto.Marshal(record)
		if err != nil {
			return -1, err
		}

		binary.Write(&buf, binary.BigEndian, int32(len(recordBytes)))
		buf.Write(recordBytes)
		if record.Type == EntryType {
			offsets = append(offsets, lastOffset)
		}
		lastOffset += int64(4 + len(recordBytes))
	}

	if _, err := buf.WriteTo(s.f); err != nil {
		return -1, err
	}
	if err := s.f.Sync(); err != nil {
		return -1, err
	}

	s.entryOffsets = append(s.entryOffsets, offsets...)
	s.nextIndex += len(offsets)
	s.lastOffset = lastOffset
	return s.nextIndex, nil
}

func openWal(dir string, softSegmentSize int) (*wal, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	w := &wal{
		dir:             dir,
		softSegmentSize: int64(softSegmentSize),
		segments:        make([]*segment, 0),
		crcTable:        crc32.MakeTable(crc32.Castagnoli),
	}

	// Find segments.
	var segments []*segment
	for _, file := range files {
		if file.IsDir() {
			log.Printf("Warning: unexpected directory found in WAL directory: %s", file.Name())
			continue
		}

		var segNum, firstIndex int
		if n, err := fmt.Sscanf(file.Name(), "log_%d_%d.dat", &segNum, &firstIndex); err != nil || n != 2 {
			log.Printf("Warning: unexpected file in WAL directory: %s", file.Name())
			continue
		}

		seg := &segment{
			w:            w,
			number:       segNum,
			firstIndex:   firstIndex,
			nextIndex:    firstIndex,
			fname:        path.Join(w.dir, file.Name()),
			entryOffsets: []int64{},
		}

		seg.f, err = os.OpenFile(seg.fname, os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			return nil, err
		}

		reader := newBufferedReader(newReaderAt(seg.f, 0))

		// Expect header record at the beginning.
		var recordLen int32
		if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
			return nil, err
		}

		headerBytes := make([]byte, recordLen)
		if _, err := reader.Read(headerBytes); err != nil {
			return nil, err
		}

		record, err := w.decodeRecord(headerBytes)
		if err != nil {
			return nil, err
		}
		if record.Type != HeaderType {
			return nil, fmt.Errorf("unexpected record type: %d", record.Type)
		}

		var header pb.WalSegmentHeader
		if err := proto.Unmarshal(record.Data, &header); err != nil {
			return nil, err
		}
		if header.Magic != walMagic {
			return nil, fmt.Errorf("invalid WAL magic number in %s", seg.fname)
		}
		if header.Version != walVersion {
			return nil, fmt.Errorf("unsupported WAL version in %s", seg.fname)
		}

		correctedSegmentNumber := seg.number
		correctedFirstIndex := seg.firstIndex
		if int(header.SegmentNumber) != seg.number {
			log.Printf(
				"Warning: segment number from file name (%s) disagrees with header's (%d), believing the latter",
				seg.fname, header.SegmentNumber)
			correctedSegmentNumber = int(header.SegmentNumber)
		}
		if int(header.FirstIndex) != seg.firstIndex {
			log.Printf(
				"Warning: first index from file name (%s) disagrees with header's (%d), believing the latter",
				seg.fname, header.FirstIndex)
			correctedFirstIndex = int(header.FirstIndex)
		}

		seg.lastOffset = int64(4 + recordLen)

		if correctedSegmentNumber != seg.number || correctedFirstIndex != seg.firstIndex {
			err := seg.f.Close()
			if err != nil {
				return nil, err
			}

			seg.number = correctedSegmentNumber
			seg.firstIndex = correctedFirstIndex

			prevFname := seg.fname
			seg.fname = path.Join(w.dir, fmt.Sprintf("log_%d_%d.dat", seg.number, seg.firstIndex))
			if err := os.Rename(prevFname, seg.fname); err != nil {
				return nil, err
			}

			seg.f, err = os.OpenFile(seg.fname, os.O_APPEND|os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			if _, err := seg.f.Seek(seg.lastOffset, io.SeekStart); err != nil {
				return nil, err
			}

			reader = newBufferedReader(newReaderAt(seg.f, seg.lastOffset))
		}

		for {
			var recordLen int32
			if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			recordBytes := make([]byte, recordLen)
			if _, err := reader.Read(recordBytes); err != nil {
				return nil, err
			}

			record, err := w.decodeRecord(recordBytes)
			if err != nil {
				return nil, err
			}

			switch record.Type {
			case EntryType:
				seg.entryOffsets = append(seg.entryOffsets, seg.lastOffset)
				seg.nextIndex++
			case StateType:
				var state pb.PersistedState
				if err := proto.Unmarshal(record.Data, &state); err != nil {
					return nil, err
				}
				w.lastState = &state
			case HeaderType:
				return nil, fmt.Errorf("unexpected header record found in segment %s", seg.fname)
			default:
				return nil, fmt.Errorf("unexpected record type: %d", record.Type)
			}
			seg.lastOffset += int64(4 + recordLen)
		}
		w.segments = append(w.segments, seg)
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
					prev.fname, prev.nextIndex, seg.fname, seg.firstIndex)
			}
		}
	}

	tail, err := w.TailEntry()
	if err != nil {
		return nil, err
	}
	if tail != nil {
		w.lastLogTerm = int(tail.Term)
	} else {
		w.lastLogTerm = -1
	}

	if len(w.segments) > 0 {
		w.tail = w.segments[len(w.segments)-1]
	} else if err := w.appendSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *wal) verify(record *pb.Record) error {
	dataCrc32 := w.crc32Of(record.Data)
	if dataCrc32 != record.Crc32 {
		return fmt.Errorf("mismatching CRC32s computed: %x, read: %x", dataCrc32, record.Crc32)
	}
	return nil
}

func (w *wal) decodeRecord(recordBytes []byte) (*pb.Record, error) {
	var record pb.Record
	if err := proto.Unmarshal(recordBytes, &record); err != nil {
		return nil, err
	}
	if err := w.verify(&record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *wal) GetState() *pb.PersistedState {
	return w.lastState
}

func (w *wal) SetState(state *pb.PersistedState) error {
	if w.closed {
		return errClosed
	}

	if proto.Equal(state, w.lastState) {
		return nil
	}

	if err := w.tail.appendState(state); err != nil {
		return err
	}
	if err := w.appendSegmentIfNeeded(); err != nil {
		return err
	}
	w.lastState = state
	return nil
}

func (w *wal) Append(state *pb.PersistedState, entries []*pb.LogEntry) (int, error) {
	if w.closed {
		return -1, errClosed
	}

	if proto.Equal(state, w.lastState) {
		state = nil // Don't append.
	}

	nextIndex := w.tail.nextIndex
	for _, entry := range entries {
		entry.Index = int32(nextIndex)
		nextIndex++
	}
	nextIndex, err := w.tail.append(state, entries)
	if err != nil {
		return -1, err
	}

	if state != nil {
		w.lastState = state
	}
	if len(entries) > 0 {
		w.lastLogTerm = int(entries[len(entries)-1].Term)
	}

	if err := w.appendSegmentIfNeeded(); err != nil {
		return -1, err
	}
	return nextIndex, nil
}

func (w *wal) AppendCommands(state *pb.PersistedState, commands [][]byte) ([]*pb.LogEntry, error) {
	if w.closed {
		return nil, errClosed
	}

	entries := toLogEntries(int(state.CurrentTerm), w.tail.nextIndex, commands)
	nextIndex, err := w.tail.append(state, entries)
	if len(entries) > 0 && nextIndex != int(entries[len(entries)-1].Index)+1 {
		log.Fatalf("Next indices not equal, expected %d got %d", entries[len(entries)-1].Index+1, nextIndex)
	}
	if err != nil {
		return nil, err
	}

	w.lastState = state
	if len(entries) > 0 {
		w.lastLogTerm = int(entries[len(entries)-1].Term)
	}

	if err := w.appendSegmentIfNeeded(); err != nil {
		return nil, err
	}
	return entries, nil
}

func (w *wal) appendSegmentIfNeeded() error {
	if w.tail == nil || w.tail.lastOffset >= w.softSegmentSize {
		return w.appendSegment()
	}
	return nil
}

func (w *wal) appendSegment() error {
	firstIndex := 0
	segNumber := 0
	var lastState *pb.PersistedState
	if len(w.segments) > 0 {
		firstIndex = w.tail.nextIndex
		segNumber = w.tail.number + 1
		lastState = w.lastState
	}

	seg := &segment{
		w:            w,
		number:       segNumber,
		firstIndex:   firstIndex,
		nextIndex:    firstIndex,
		fname:        path.Join(w.dir, fmt.Sprintf("log_%d_%d.dat", segNumber, firstIndex)),
		entryOffsets: []int64{},
	}

	var buf bytes.Buffer
	headerRecord, err := w.recordOf(HeaderType, &pb.WalSegmentHeader{
		Magic:         walMagic,
		Version:       walVersion,
		Flags:         0,
		SegmentNumber: int32(seg.number),
		FirstIndex:    int32(firstIndex),
	})
	if err != nil {
		return err
	}
	recordBytes, err := proto.Marshal(headerRecord)
	if err != nil {
		return err
	}
	binary.Write(&buf, binary.BigEndian, int32(len(recordBytes)))
	buf.Write(recordBytes)

	// Append lastState to the new segment if available.
	if lastState != nil {
		stateRecord, err := w.recordOf(StateType, lastState)
		if err != nil {
			return err
		}
		recordBytes, err := proto.Marshal(stateRecord)
		if err != nil {
			return err
		}
		binary.Write(&buf, binary.BigEndian, int32(len(recordBytes)))
		buf.Write(recordBytes)
	}

	tempFname := seg.fname + ".tmp"
	tempF, err := os.Create(tempFname)
	if err != nil {
		return err
	}
	writtenCount := buf.Len()
	err = func() error {
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
		tempF.Close()
		os.Remove(tempFname)
		return err
	}

	if err := os.Rename(tempFname, seg.fname); err != nil {
		return err
	}

	// TODO correct fileMode?
	f, err := os.OpenFile(seg.fname, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	seg.f = f
	seg.lastOffset = int64(writtenCount)
	w.segments = append(w.segments, seg)
	w.tail = seg
	return nil
}

func (w *wal) TruncateEntriesFrom(index int) error {
	if w.closed {
		return errClosed
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		return err
	}

	for i := len(w.segments) - 1; i > segIndex; i-- {
		if err := w.segments[i].delete(); err != nil {
			return err
		}
		w.segments[i] = nil
	}
	w.segments = w.segments[0 : segIndex+1]
	w.tail = w.segments[segIndex]
	if err := w.tail.truncateEntriesFrom(index); err != nil {
		return err
	}
	if err := w.appendSegmentIfNeeded(); err != nil {
		return err
	}
	return nil
}

func (w *wal) EntryCount() int {
	count := 0
	for _, seg := range w.segments {
		count += seg.entryCount()
	}
	return count
}

func (w *wal) GetEntry(index int) (*pb.LogEntry, error) {
	if w.closed {
		return nil, errClosed
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		return nil, err
	}
	return w.segments[segIndex].getEntry(index)
}

func (w *wal) GetEntryTerm(index int) (int, error) {
	entry, err := w.GetEntry(index)
	if err != nil {
		return -1, err
	}
	return int(entry.Term), nil
}

func (w *wal) HeadEntry() (*pb.LogEntry, error) {
	if w.closed {
		return nil, errClosed
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
		return nil, errClosed
	}

	// Search for the first segment from the end that has an entry.
	for i := len(w.segments) - 1; i >= 0; i-- {
		seg := w.segments[i]
		entry, err := seg.tailEntry()
		if err != nil {
			return nil, err
		}
		if entry != nil {
			return entry, nil
		}
	}
	return nil, nil
}

func (w *wal) LastLogIndexAndTerm() (int, int) {
	return w.tail.nextIndex - 1, w.lastLogTerm
}

func (w *wal) GetEntriesFrom(index int) ([]*pb.LogEntry, error) {
	if w.closed {
		return nil, errClosed
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		return nil, err
	}

	entries, err := w.segments[segIndex].getEntriesFrom(index)
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

func (w *wal) Close() error {
	if w.closed {
		return nil
	}

	var errs []error
	for _, seg := range w.segments {
		if err := seg.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	w.closed = true
	return errors.Join(errs...)
}

func (w *wal) findSegment(entryIndex int) (int, error) {
	lo, hi := 0, len(w.segments)
	for lo < hi {
		mid := (lo + hi) / 2
		if entryIndex >= w.segments[mid].nextIndex {
			lo = mid + 1
		} else if entryIndex < w.segments[mid].firstIndex || w.segments[mid].entryCount() == 0 {
			hi = mid
		} else {
			return mid, nil
		}
	}
	return -1, indexOutOfRange(entryIndex)
}

func (w *wal) recordOf(recordType uint32, msg proto.Message) (*pb.Record, error) {
	if data, err := proto.Marshal(msg); err != nil {
		return nil, err
	} else {
		return &pb.Record{Type: recordType, Crc32: w.crc32Of(data), Data: data}, err
	}
}

func (w *wal) crc32Of(data []byte) uint32 {
	hash := crc32.New(w.crcTable)
	hash.Write(data)
	return hash.Sum32()
}
