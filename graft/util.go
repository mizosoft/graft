package graft

import (
	"errors"
	"io"
	"log"
	"os"
	"sync"

	"google.golang.org/protobuf/proto"
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
		return errors.Join(err, currErr)
	}
	return currErr
}

func closeOnErr(closer io.Closer, curErr error) error {
	err := closer.Close()
	if err != nil {
		return errors.Join(err, curErr)
	}
	return curErr
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
