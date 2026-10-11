package graft

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path"
	"strings"
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

// A failure to close is attached after the error being returned, which comes back as is if closing succeeds, or if the
// closer is closed already.
func TestCloseOnErr(t *testing.T) {
	err := errors.New("primary")
	assert.Equal(t, closeOnErr(errCloser{}, err), err)
	assert.Equal(t, closeOnErr(errCloser{errors.New("close failed")}, err).Error(), "primary\nclose failed")
	closed := &os.PathError{Op: "close", Path: "file", Err: os.ErrClosed}
	assert.Equal(t, closeOnErr(errCloser{closed}, err), err)
}

// A failure to remove is attached after the error being returned, which comes back as is if removing succeeds.
func TestRemoveOnErr(t *testing.T) {
	err := errors.New("primary")
	fname := path.Join(t.TempDir(), "file")
	assert.NilError(t, os.WriteFile(fname, nil, 0644))
	assert.Equal(t, removeOnErr(fname, err), err)

	joined := removeOnErr(fname, err) // Removed already.
	assert.ErrorIs(t, joined, os.ErrNotExist)
	assert.Assert(t, strings.HasPrefix(joined.Error(), "primary\n"), joined.Error())
}

type errCloser struct{ err error }

func (c errCloser) Close() error { return c.err }
