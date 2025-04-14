package graft

import (
	"bytes"
	"io"
	"os"
	"path"
	"testing"

	"gotest.tools/v3/assert"
)

func TestOffsetReader(t *testing.T) {
	dir := t.TempDir()
	fname := path.Join(dir, "tmp.txt")

	f, err := os.Create(fname)
	assert.NilError(t, err)
	defer f.Close()

	_, err = f.WriteString("1234567890")
	assert.NilError(t, err)

	reader := newReaderAt(f, 2)
	bytes := make([]byte, 4)
	_, err = reader.Read(bytes)
	assert.NilError(t, err)
	assert.Equal(t, string(bytes), "3456")
	_, err = reader.Read(bytes)
	assert.NilError(t, err)
	assert.Equal(t, string(bytes), "7890")
}

func TestBufferedReader(t *testing.T) {
	bufferSize := 4
	reader := newBufferedReaderWithSize(bytes.NewReader([]byte("123456789012")), bufferSize)

	p := make([]byte, 2)
	n, err := reader.Read(p)
	assert.NilError(t, err)
	assert.Equal(t, n, 2)
	assert.Equal(t, string(p), "12")

	p = make([]byte, 6)
	n, err = reader.Read(p)
	assert.NilError(t, err)
	assert.Equal(t, n, 6)
	assert.Equal(t, string(p), "345678")

	p = make([]byte, 2)
	n, err = reader.Read(p)
	assert.NilError(t, err)
	assert.Equal(t, n, 2)
	assert.Equal(t, string(p), "90")

	p = make([]byte, 4)
	n, err = reader.Read(p)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, n, 2)
	assert.Equal(t, string(p[0:2]), "12")

	p = make([]byte, 4)
	n, err = reader.Read(p)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, n, 0)
}

func TestBufferedReaderFallThrough(t *testing.T) {
	bufferSize := 4
	reader := newBufferedReaderWithSize(bytes.NewReader([]byte("123456789012")), bufferSize)

	p := make([]byte, 2)
	n, err := reader.Read(p)
	assert.NilError(t, err)
	assert.Equal(t, n, 2)
	assert.Equal(t, string(p), "12")

	// Read buffered data & fallthrough buffering.
	p = make([]byte, 8)
	n, err = reader.Read(p)
	assert.NilError(t, err)
	assert.Equal(t, n, 8)
	assert.Equal(t, string(p), "34567890")

	p = make([]byte, 4)
	n, err = reader.Read(p)
	assert.ErrorIs(t, err, io.EOF)
	assert.Equal(t, n, 2)
	assert.Equal(t, string(p[0:2]), "12")
}
