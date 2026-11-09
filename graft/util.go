package graft

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var snapshotCRCTable = crc32.MakeTable(crc32.Castagnoli)

type checksumVerifyingReader struct {
	r        io.Reader
	h        uint32
	expected uint32
}

func (r *checksumVerifyingReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.h = crc32.Update(r.h, snapshotCRCTable, p[:n])
	}
	if err == io.EOF && r.h != r.expected {
		return n, ErrCorrupt
	}
	return n, err
}

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

func newReaderAt(f *os.File, offset int64) io.Reader {
	return &offsetFileReader{f: f, offset: offset}
}

const bufferSize = 16 * 1024

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

func protoMarshal(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		log.Panicf("failed to marshal record: %v", err)
	}
	return data
}

func protoUnmarshal(data []byte, msg proto.Message) {
	if err := proto.Unmarshal(data, msg); err != nil {
		log.Panicf("failed to unmarshal record: %v", err)
	}
}

func removeOnErr(fname string, currErr error) error {
	err := os.Remove(fname)
	if err != nil {
		return errors.Join(currErr, err)
	}
	return currErr
}

func closeOnErr(closer io.Closer, curErr error) error {
	// Being closed already, e.g. by the step that failed, isn't a failure to report.
	if err := closer.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return errors.Join(curErr, err)
	}
	return curErr
}

func closeQuietly(closer io.Closer, logger *zap.SugaredLogger) {
	if err := closer.Close(); err != nil {
		logger.Errorw("Error closing", "closer", fmt.Sprintf("%T", closer), zap.Error(err))
	}
}

func cloneMsgs[T proto.Message](msgs []T) []T {
	cloned := make([]T, len(msgs))
	for i, msg := range msgs {
		cloned[i] = cloneMsg(msg)
	}
	return cloned
}

func cloneMsg[T proto.Message](msg T) T {
	return proto.Clone(msg).(T)
}

// Used when the assigned slice is different from the slice appended to.
func copyAppend[T any](slice []T, elems ...T) []T {
	return append(append([]T(nil), slice...), elems...)
}
