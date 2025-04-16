package graft

import (
	"io"
	"os"
	"sync"

	"github.com/mizosoft/graft/pb"
)

type uncopyable struct {
	_ sync.Mutex
}

type offsetFileReader struct {
	f      *os.File
	offset int64
}

func (r *offsetFileReader) Read(p []byte) (int, error) {
	n, err := r.f.ReadAt(p, r.offset)
	r.offset += int64(n)
	return n, err
}

type bufferedReader struct {
	_ uncopyable

	buf    []byte
	pos    int
	count  int
	err    error
	reader io.Reader
}

const bufferSize = 8096

func newReaderAt(f *os.File, offset int64) io.Reader {
	return &offsetFileReader{f: f, offset: offset}
}

func newBufferedReader(reader io.Reader) io.Reader {
	return &bufferedReader{
		buf:    make([]byte, bufferSize),
		reader: reader,
	}
}

func newBufferedReaderWithSize(reader io.Reader, bufSize int) io.Reader {
	return &bufferedReader{
		buf:    make([]byte, bufSize),
		reader: reader,
	}
}

func (b *bufferedReader) Read(p []byte) (int, error) {
	for n := 0; ; {
		c := copy(p[n:], b.buf[b.pos:b.count])
		n += c
		b.pos += c
		if n == len(p) {
			return n, nil
		}

		if b.err != nil {
			return n, b.err
		}

		if len(p)-n > len(b.buf) {
			// No need to buffer.
			cp, err := b.reader.Read(p[n:])
			b.err = err
			return n + cp, err
		} else {
			b.pos = 0
			b.count, b.err = b.reader.Read(b.buf)
		}
	}
}

func toLogEntries(term int, nextIndex int, commands [][]byte) []*pb.LogEntry {
	entries := make([]*pb.LogEntry, len(commands))
	for i, command := range commands {
		entries[i] = &pb.LogEntry{Term: int32(term), Index: int32(nextIndex), Command: command}
		nextIndex++
	}
	return entries
}
