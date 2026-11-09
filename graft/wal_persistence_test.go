package graft_test

import (
	"fmt"
	"testing"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/testing/persistencetest"
)

func TestWalPersistence(t *testing.T) {
	for _, memoryMapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("MemoryMapped=%t", memoryMapped), func(t *testing.T) {
			persistencetest.Run(t, persistencetest.Backend{
				Open: func(dir string) (graft.Persistence, error) {
					// Small segments, so the longer logs span several.
					return graft.OpenWal(graft.WalOptions{Dir: dir, SegmentSize: 1024, MemoryMapped: memoryMapped})
				},
			})
		})
	}
}
