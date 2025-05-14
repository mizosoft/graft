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
	headerRecordType = iota
	stateRecordType
	entryRecordType
	snapshotMetadataRecordType
)

const (
	walMagic   uint64 = 0x6420655767271765
	walVersion int32  = 1
)

var (
	errClosed = errors.New("closed")
)

type wal struct {
	dir                       string
	softSegmentSize           int64
	segments                  []*segment
	tail                      *segment
	crcTable                  *crc32.Table
	closed                    bool
	lastState                 *pb.PersistedState
	lastSnapshot              *pb.SnapshotMetadata // TODO we might want to make sure this is appended on truncation.
	firstLogTerm, lastLogTerm int64
}

type segment struct {
	w                     *wal
	number                int
	firstIndex, nextIndex int64 // First & next entry indices.
	fpath                 string
	f                     *os.File
	entryOffsets          []int64
	lastOffset            int64
}

func segFileName(number int, firstIndex int64) string {
	return fmt.Sprintf("log_%d_%d.dat", number, firstIndex)
}

func snapFileName(metadata *pb.SnapshotMetadata) string {
	return fmt.Sprintf("snap_%d_%d.dat", metadata.LastAppliedIndex, metadata.LastAppliedTerm)
}

func appendRecord(f *os.File, record *pb.WalRecord) (int, error) {
	recordBytes := protoMarshal(record)
	byteCount := 4 + len(recordBytes)
	chunk := make([]byte, byteCount)
	binary.BigEndian.PutUint32(chunk, uint32(len(recordBytes)))
	copy(chunk[4:], recordBytes)
	if _, err := f.Write(chunk); err != nil {
		return 0, err
	}
	if err := f.Sync(); err != nil {
		return 0, err
	}
	return byteCount, nil
}

func (s *segment) entryCount() int {
	return len(s.entryOffsets)
}

func (s *segment) getEntry(index int64) (*pb.LogEntry, error) {
	localIndex := int(index - s.firstIndex)
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
	if record.Type != entryRecordType {
		panic(fmt.Errorf("unexpected record type when expecting an entry: %d", record.Type))
	}

	var entry pb.LogEntry
	if err := proto.Unmarshal(record.Data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (s *segment) getEntriesFrom(from int64) ([]*pb.LogEntry, error) {
	localFrom := int(from - s.firstIndex)
	if localFrom < 0 || localFrom >= s.entryCount() {
		return []*pb.LogEntry{}, nil
	}

	reader := newBufferedReader(newReaderAt(s.f, s.entryOffsets[localFrom]))
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
		if record.Type != entryRecordType {
			continue // Might have some non-entry records.
		}

		var entry pb.LogEntry
		if err := proto.Unmarshal(record.Data, &entry); err != nil {
			return nil, err
		}
		entries = append(entries, &entry)
	}
}

func (s *segment) getEntriesTo(to int64) ([]*pb.LogEntry, error) {
	localTo := int(to - s.firstIndex)
	if localTo < 0 || localTo >= s.entryCount() {
		return []*pb.LogEntry{}, nil
	}

	reader := newBufferedReader(newReaderAt(s.f, s.entryOffsets[0]))
	var entries []*pb.LogEntry
	for index := s.firstIndex; index <= to; {
		var recordLen int32
		if err := binary.Read(reader, binary.BigEndian, &recordLen); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.ErrUnexpectedEOF
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

	if err := s.f.Sync(); err != nil {
		return err
	}
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

	// Create a new segment file with the remaining entries.

	newFirstIndex := index + 1
	newFpath := path.Join(s.w.dir, segFileName(s.number, newFirstIndex))
	tempNewFpath := newFpath + ".tmp"
	tempF, err := os.Create(tempNewFpath)
	if err != nil {
		return false, err
	}

	var newLastOffset int64
	var offsetDiff int64
	err = func() error {
		// Append new header record.
		nHeader, err := appendRecord(tempF, s.w.recordOf(headerRecordType, &pb.WalSegmentHeader{
			Magic:         walMagic,
			Version:       walVersion,
			Flags:         0,
			SegmentNumber: int32(s.number),
			FirstIndex:    newFirstIndex,
		}))
		if err != nil {
			return err
		}

		// Copy remaining records.
		copyOffset := s.entryOffsets[localIndex+1]
		if _, err := s.f.Seek(copyOffset, io.SeekStart); err != nil {
			return err
		}

		nCopied, err := io.Copy(tempF, s.f)
		if err != nil {
			return err
		}

		// We want oldOffset + offsetDiff = newOffset.
		offsetDiff = int64(nHeader) - copyOffset
		newLastOffset = int64(nHeader) + nCopied
		return nil
	}()
	if err != nil {
		return false, removeOnErr(tempNewFpath, closeOnErr(tempF, err))
	}

	// Do atomic replace.
	if err := tempF.Close(); err != nil {
		return false, err
	}
	if err := os.Rename(tempNewFpath, newFpath); err != nil {
		return false, err
	}

	newF, err := os.OpenFile(newFpath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return false, err
	}
	if _, err := newF.Seek(newLastOffset, io.SeekStart); err != nil {
		return false, closeOnErr(newF, err)
	}

	s.entryOffsets = s.entryOffsets[localIndex+1:]
	for i := range s.entryOffsets {
		s.entryOffsets[i] += offsetDiff
	}

	// Shift the offsets by the amount we deleted.

	s.firstIndex = newFirstIndex
	s.fpath, s.f = newFpath, newF
	s.lastOffset = newLastOffset
	return false, nil
}

func (s *segment) delete() error {
	if err := s.f.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.fpath); err != nil {
		return err
	}
	return nil
}

func (s *segment) appendRecord(recordType uint32, msg proto.Message) error {
	n, err := appendRecord(s.f, s.w.recordOf(recordType, msg))
	if err != nil {
		return err
	}
	s.lastOffset += int64(n)
	return nil
}

func (s *segment) appendState(state *pb.PersistedState) error {
	return s.appendRecord(stateRecordType, state)
}

func (s *segment) append(state *pb.PersistedState, entries []*pb.LogEntry) (int64, error) {
	if state == nil && len(entries) == 0 {
		return s.nextIndex, nil
	}

	if len(entries) == 0 {
		return s.nextIndex, s.appendState(state)
	} else if entries[0].Index != s.nextIndex {
		return -1, fmt.Errorf(
			"appending entries with mismatching index continuity, expected %d, got %d", s.nextIndex, entries[0].Term)
	}

	records := make([]*pb.WalRecord, 0, 1+len(entries))
	for _, entry := range entries {
		records = append(records, s.w.recordOf(entryRecordType, entry))
	}

	if state != nil {
		records = append(records, s.w.recordOf(stateRecordType, state))
	}

	lastOffset := s.lastOffset
	var offsets []int64
	var buf bytes.Buffer
	for _, record := range records {
		recordBytes := protoMarshal(record)
		binary.Write(&buf, binary.BigEndian, int32(len(recordBytes)))
		buf.Write(recordBytes)
		if record.Type == entryRecordType {
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
	s.nextIndex += int64(len(offsets))
	s.lastOffset = lastOffset
	return s.nextIndex, nil
}

func openWal(dir string, softSegmentSize int64) (*wal, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	w := &wal{
		dir:             dir,
		softSegmentSize: softSegmentSize,
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

		var segNum int
		var firstIndex int64
		if n, err := fmt.Sscanf(file.Name(), "log_%d_%d.dat", &segNum, &firstIndex); err != nil || n != 2 {
			log.Printf("Warning: unexpected file in WAL directory: %s", file.Name())
			continue
		}

		seg := &segment{
			w:            w,
			number:       segNum,
			firstIndex:   firstIndex,
			nextIndex:    firstIndex,
			fpath:        path.Join(dir, file.Name()),
			entryOffsets: []int64{},
		}

		// TODO don't forget to close uneeded open files when errors happen.
		seg.f, err = os.OpenFile(seg.fpath, os.O_APPEND|os.O_RDWR, 0644)
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
		if record.Type != headerRecordType {
			return nil, fmt.Errorf("unexpected record type: %d", record.Type)
		}

		var header pb.WalSegmentHeader
		if err := proto.Unmarshal(record.Data, &header); err != nil {
			return nil, err
		}
		if header.Magic != walMagic {
			return nil, fmt.Errorf("invalid WAL magic number in %s", seg.fpath)
		}
		if header.Version != walVersion {
			return nil, fmt.Errorf("unsupported WAL version in %s", seg.fpath)
		}

		correctedSegmentNumber := seg.number
		correctedFirstIndex := seg.firstIndex
		if int(header.SegmentNumber) != seg.number {
			log.Printf(
				"Warning: segment number from file name (%s) disagrees with header's (%d), believing the latter",
				seg.fpath, header.SegmentNumber)
			correctedSegmentNumber = int(header.SegmentNumber)
		}
		if header.FirstIndex != seg.firstIndex {
			log.Printf(
				"Warning: first index from file name (%s) disagrees with header's (%d), believing the latter",
				seg.fpath, header.FirstIndex)
			correctedFirstIndex = header.FirstIndex
		}

		seg.lastOffset = int64(4 + recordLen)

		if correctedSegmentNumber != seg.number || correctedFirstIndex != seg.firstIndex {
			err := seg.f.Close()
			if err != nil {
				return nil, err
			}

			correctedFpath := path.Join(w.dir, segFileName(correctedSegmentNumber, correctedFirstIndex))
			if err := os.Rename(seg.fpath, correctedFpath); err != nil {
				return nil, err
			}

			f, err := os.OpenFile(correctedFpath, os.O_APPEND|os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			if _, err := f.Seek(seg.lastOffset, io.SeekStart); err != nil {
				return nil, err
			}
			seg.number, seg.firstIndex = correctedSegmentNumber, correctedFirstIndex
			seg.fpath, seg.f = correctedFpath, f

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
			case entryRecordType:
				seg.entryOffsets = append(seg.entryOffsets, seg.lastOffset)
				seg.nextIndex++
			case stateRecordType:
				var state pb.PersistedState
				if err := proto.Unmarshal(record.Data, &state); err != nil {
					return nil, err
				}
				w.lastState = &state
			case snapshotMetadataRecordType:
				var snapshot pb.SnapshotMetadata
				if err := proto.Unmarshal(record.Data, &snapshot); err != nil {
					return nil, err
				}
				w.lastSnapshot = &snapshot
			case headerRecordType:
				return nil, fmt.Errorf("unexpected header record found in segment %s", seg.fpath)
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
					prev.fpath, prev.nextIndex, seg.fpath, seg.firstIndex)
			}
		}
	}

	tail := w.TailEntry()
	if tail != nil {
		w.lastLogTerm = tail.Term
	} else {
		w.lastLogTerm = -1
	}

	head := w.HeadEntry()
	if head != nil {
		w.firstLogTerm = head.Term
	} else {
		w.firstLogTerm = -1
	}

	if len(w.segments) > 0 {
		w.tail = w.segments[len(w.segments)-1]
	} else if err := w.appendSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *wal) verify(record *pb.WalRecord) error {
	dataCrc32 := w.crc32Of(record.Data)
	if dataCrc32 != record.Crc32 {
		return fmt.Errorf("mismatching CRC32s computed: %x, read: %x", dataCrc32, record.Crc32)
	}
	return nil
}

func (w *wal) decodeRecord(recordBytes []byte) (*pb.WalRecord, error) {
	var record pb.WalRecord
	if err := proto.Unmarshal(recordBytes, &record); err != nil {
		return nil, err
	}
	if err := w.verify(&record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *wal) RetrieveState() *pb.PersistedState {
	return w.lastState
}

func (w *wal) SaveState(state *pb.PersistedState) {
	if w.closed {
		panic(errClosed)
	}

	if proto.Equal(state, w.lastState) {
		return
	}

	if err := w.tail.appendState(state); err != nil {
		panic(err)
	}
	if err := w.appendSegmentIfNeeded(); err != nil {
		panic(err)
	}
	w.lastState = state
}

func (w *wal) Append(state *pb.PersistedState, entries []*pb.LogEntry) int64 {
	if w.closed {
		panic(errClosed)
	}

	if proto.Equal(state, w.lastState) {
		state = nil // Don't append.
	}

	nextIndex := w.tail.nextIndex
	for _, entry := range entries {
		entry.Index = nextIndex
		nextIndex++
	}
	nextIndex, err := w.tail.append(state, entries)
	if err != nil {
		panic(err)
	}

	if state != nil {
		w.lastState = state
	}
	if len(entries) > 0 {
		if w.firstLogTerm < 0 {
			w.firstLogTerm = entries[0].Term
		}
		w.lastLogTerm = entries[len(entries)-1].Term
	}

	if err := w.appendSegmentIfNeeded(); err != nil {
		panic(err)
	}
	return nextIndex
}

func (w *wal) appendSegmentIfNeeded() error {
	if w.tail == nil || w.tail.lastOffset >= w.softSegmentSize {
		return w.appendSegment()
	}
	return nil
}

func (w *wal) appendSegment() error {
	firstIndex := int64(0)
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
		fpath:        path.Join(w.dir, segFileName(segNumber, firstIndex)),
		entryOffsets: []int64{},
	}

	var buf bytes.Buffer
	recordBytes := protoMarshal(
		w.recordOf(headerRecordType, &pb.WalSegmentHeader{
			Magic:         walMagic,
			Version:       walVersion,
			Flags:         0,
			SegmentNumber: int32(seg.number),
			FirstIndex:    firstIndex,
		}))
	binary.Write(&buf, binary.BigEndian, int32(len(recordBytes)))
	buf.Write(recordBytes)

	// Append lastState to the new segment if available.
	if lastState != nil {
		recordBytes := protoMarshal(w.recordOf(stateRecordType, lastState))
		binary.Write(&buf, binary.BigEndian, int32(len(recordBytes)))
		buf.Write(recordBytes)
	}

	tempFpath := seg.fpath + ".tmp"
	tempF, err := os.Create(tempFpath)
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
		return removeOnErr(tempFpath, closeOnErr(tempF, err))
	}

	if err := os.Rename(tempFpath, seg.fpath); err != nil {
		return err
	}

	// TODO correct fileMode?
	f, err := os.OpenFile(seg.fpath, os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	seg.f = f
	seg.lastOffset = int64(writtenCount)
	w.segments = append(w.segments, seg)
	w.tail = seg
	return nil
}

func (w *wal) TruncateEntriesFrom(index int64) {
	if w.closed {
		panic(errClosed)
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		panic(err)
	}

	removedSegments := w.segments[segIndex+1:]
	w.segments = w.segments[0 : segIndex+1]
	w.tail = w.segments[segIndex]

	for _, seg := range removedSegments {
		if err := seg.delete(); err != nil {
			panic(err)
		}
	}
	if err := w.tail.truncateEntriesFrom(index); err != nil {
		panic(err)
	}
	if err := w.appendSegmentIfNeeded(); err != nil {
		panic(err)
	}

	// Make sure we have lastState appended since it might have been truncated.
	if w.lastState != nil {
		err := w.tail.appendState(w.lastState)
		if err != nil {
			panic(err)
		}
	}
}

func (w *wal) TruncateEntriesTo(index int64) {
	if w.closed {
		panic(errClosed)
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		panic(err)
	}

	removedSegments := w.segments[:segIndex]
	w.segments = w.segments[segIndex:]

	for _, seg := range removedSegments {
		if err := seg.delete(); err != nil {
			panic(err)
		}
	}

	removeHead, err := w.segments[0].truncateEntriesTo(index)
	if err != nil {
		panic(err)
	}
	if removeHead {
		w.segments = w.segments[1:]
	}

	if err := w.appendSegmentIfNeeded(); err != nil {
		panic(err)
	}

	// Make sure we have lastState appended since it might have been truncated.
	if w.lastState != nil {
		err := w.tail.appendState(w.lastState)
		if err != nil {
			panic(err)
		}
	}
}

func (w *wal) EntryCount() int64 {
	count := int64(0)
	for _, seg := range w.segments {
		count += int64(seg.entryCount())
	}
	return count
}

func (w *wal) GetEntry(index int64) *pb.LogEntry {
	if w.closed {
		panic(errClosed)
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		panic(err)
	}

	entry, err := w.segments[segIndex].getEntry(index)
	if err != nil {
		panic(err)
	}
	return entry
}

func (w *wal) GetEntryTerm(index int64) int64 {
	return w.GetEntry(index).Term
}

func (w *wal) GetEntries(from, to int64) []*pb.LogEntry {
	if from > to {
		panic(fmt.Errorf("from (%d) must be smaller than or equal to (%d)", from, to))
	}

	if from == to {
		return []*pb.LogEntry{w.GetEntry(from)}
	}

	firstSegIndex, err := w.findSegment(from)
	if err != nil {
		panic(err)
	}

	lastSegIndex, err := w.findSegment(to)
	if err != nil {
		panic(err)
	}

	entries, err := w.segments[firstSegIndex].getEntriesFrom(from)
	if err != nil {
		panic(err)
	}

	if firstSegIndex == lastSegIndex {
		return entries[0 : to-from+1]
	} else {
		for i := firstSegIndex + 1; i < lastSegIndex; i++ {
			seg := w.segments[i]
			middleEntries, err := seg.getEntriesFrom(seg.firstIndex)
			if err != nil {
				panic(err)
			}
			entries = append(entries, middleEntries...)
		}

		tailEntries, err := w.segments[lastSegIndex].getEntriesTo(to)
		if err != nil {
			panic(err)
		}
		return append(entries, tailEntries...)
	}
}

func (w *wal) HeadEntry() *pb.LogEntry {
	if w.closed {
		panic(errClosed)
	}

	// Search for the first segment from the start that has an entry.
	for i := 0; i < len(w.segments); i++ {
		seg := w.segments[i]
		entry, err := seg.headEntry()
		if err != nil {
			panic(err)
		}
		if entry != nil {
			return entry
		}
	}
	return nil
}

func (w *wal) TailEntry() *pb.LogEntry {
	if w.closed {
		panic(errClosed)
	}

	// Search for the first segment from the end that has an entry.
	for i := len(w.segments) - 1; i >= 0; i-- {
		seg := w.segments[i]
		entry, err := seg.tailEntry()
		if err != nil {
			panic(err)
		}
		if entry != nil {
			return entry
		}
	}
	return nil
}

func (w *wal) GetEntriesFrom(index int64) []*pb.LogEntry {
	if w.closed {
		panic(errClosed)
	}

	segIndex, err := w.findSegment(index)
	if err != nil {
		panic(err)
	}

	entries, err := w.segments[segIndex].getEntriesFrom(index)
	if err != nil {
		panic(err)
	}

	for i := segIndex + 1; i < len(w.segments); i++ {
		seg := w.segments[i]
		segEntries, err := seg.getEntriesFrom(seg.firstIndex)
		if err != nil {
			panic(err)
		}
		entries = append(entries, segEntries...)
	}
	return entries
}

func (w *wal) SaveSnapshot(snapshot Snapshot) {
	if w.closed {
		panic(errClosed)
	}

	metadata := snapshot.Metadata()
	fpath := path.Join(w.dir, snapFileName(metadata))
	tempFpath := fpath + ".tmp"
	tempF, err := os.Create(tempFpath)
	if err != nil {
		panic(err)
	}
	defer tempF.Close()

	if _, err := tempF.Write(snapshot.Data()); err != nil {
		panic(err)
	}
	if err := tempF.Sync(); err != nil {
		panic(err)
	}
	if err := tempF.Close(); err != nil {
		panic(err)
	}
	if err := os.Rename(tempFpath, fpath); err != nil {
		panic(err)
	}
	if err := w.tail.appendRecord(snapshotMetadataRecordType, metadata); err != nil {
		panic(err)
	}
	if err := w.appendSegmentIfNeeded(); err != nil {
		panic(err)
	}
	w.lastSnapshot = metadata
}

func (w *wal) RetrieveSnapshot() Snapshot {
	if w.closed {
		panic(errClosed)
	}

	metadata := w.lastSnapshot
	if metadata == nil {
		return nil
	}

	data, err := os.ReadFile(path.Join(w.dir, snapFileName(metadata)))
	if err != nil {
		panic(err)
	}
	return NewSnapshot(metadata, data)
}

func (w *wal) SnapshotMetadata() *pb.SnapshotMetadata {
	return w.lastSnapshot
}

func (w *wal) FirstLogIndexAndTerm() (int64, int64) {
	if w.EntryCount() == 0 {
		return -1, -1
	}

	// Node that this specific segment might not carry any entries but firstIndex carries over to first segment
	// with entries.
	return w.segments[0].firstIndex, w.firstLogTerm
}

func (w *wal) LastLogIndexAndTerm() (int64, int64) {
	if w.EntryCount() == 0 {
		return -1, -1
	}

	// Node that this specific segment might not carry any entries but nextIndex is carried over from last segment
	// with entries.
	return w.tail.nextIndex - 1, w.lastLogTerm
}

func (w *wal) Close() {
	if w.closed {
		return
	}

	var errs []error
	for _, seg := range w.segments {
		if err := seg.f.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	w.closed = true

	if len(errs) > 0 {
		log.Printf("wal: close errors: %v\n", errs)
	}
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
	return -1, indexOutOfRange(entryIndex)
}

func (w *wal) recordOf(recordType uint32, msg proto.Message) *pb.WalRecord {
	data := protoMarshal(msg)
	return &pb.WalRecord{Type: recordType, Crc32: w.crc32Of(data), Data: data}
}

func (w *wal) crc32Of(data []byte) uint32 {
	hash := crc32.New(w.crcTable)
	hash.Write(data)
	return hash.Sum32()
}
