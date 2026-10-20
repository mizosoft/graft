package badger

import (
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/testing/persistencetest"
	"gotest.tools/v3/assert"
)

func TestWalErrorCases(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenBadgerPersistence(dir, nil)
	defer w.Close()
	assert.NilError(t, err)

	_, err = w.GetEntry(100)
	assert.Assert(t, err != nil)

	// Test using closed WAL
	w.Close()

	_, err = w.GetEntry(0)
	assert.Assert(t, err != nil)
}

func TestBadgerPersistence(t *testing.T) {
	persistencetest.Run(t, persistencetest.Backend{
		Open: func(dir string) (graft.Persistence, error) { return OpenBadgerPersistence(dir, nil) },
		Skip: map[string]string{"AppendContinuesAfterFullCompaction": "#24: an emptied log rewinds"},
	})
}
