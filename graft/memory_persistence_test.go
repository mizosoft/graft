package graft_test

import (
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/testing/persistencetest"
)

func TestMemoryPersistence(t *testing.T) {
	persistencetest.Run(t, persistencetest.Backend{
		Open:     func(string) (graft.Persistence, error) { return graft.MemoryPersistence(), nil },
		Volatile: true,
	})
}
