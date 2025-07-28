package benchmarks

import (
	"fmt"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
	"math/rand"
	"testing"
)

//func BenchmarkWalPersistence(b *testing.B) {
//	cluster, store := newClusterClientWithWalPersistence(b, 3, 0)
//	defer cluster.Shutdown()
//
//	b.ResetTimer()
//	for i := 0; ji < b.N; i++ {
//		_, _, err := store.Put("k", "v")
//		if err != nil {
//			b.Fatalf("put error: %v", err)
//		}
//	}
//}
//
//func BenchmarkBadgerPersistence(b *testing.B) {
//	cluster, store := newClusterClient(b, 3, 0)
//	defer cluster.Shutdown()
//
//	b.ResetTimer()
//	for i := 0; i < b.N; i++ {
//		_, _, err := store.Put("k", "v")
//		if err != nil {
//			b.Fatalf("put error: %v", err)
//		}
//	}
//}

func BenchmarkWalAppend(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 512*1024, 0, zap.NewNop())
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nextIndex := wal.Append(&pb.PersistedState{CurrentTerm: int64(entriesCount + i)}, entries)
		if nextIndex != int64((i+1)*entriesCount) {
			b.Fatalf("Append error: nextIndex=%d", nextIndex)
		}
	}
}

func BenchmarkWalSequentialScan(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 128*1024, 0, zap.NewNop())
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Span multiple segments.
	appendCount := 32
	for i := range 32 {
		wal.Append(&pb.PersistedState{CurrentTerm: int64(i)}, entries)
	}

	totalEntryCount := entriesCount * appendCount

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := range totalEntryCount {
			e := wal.GetEntry(int64(i))
			if e.Index != int64(i) {
				b.Fatalf("GetEntry error: index=%d, expected=%d", e.Index, i)
			}
		}
	}
}

func BenchmarkWalBatchedSequentialScan(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 128*1024, 0, zap.NewNop())
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Span multiple segments.
	appendCount := 32
	for i := range 32 {
		wal.Append(&pb.PersistedState{CurrentTerm: int64(i)}, entries)
	}

	totalEntryCount := entriesCount * appendCount

	// Benchmark
	b.ResetTimer()
	for _, batchSize := range []int{1024, 2048, 4096, 8192, 16384} {
		b.Run(fmt.Sprintf("batchSize=%dB", batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for i := 0; i < totalEntryCount; i += batchSize {
					es := wal.GetEntries(int64(i), int64(i+batchSize-1))
					for ei, e := range es {
						if e.Index != int64(i+ei) {
							b.Fatalf("GetEntry error: index=%d, expected=%d", e.Index, i)
						}
					}
				}
			}
		})
	}
}

func BenchmarkRandomScan(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 128*1024, 0, zap.NewNop())
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Span multiple segments.
	appendCount := 32
	for i := range 32 {
		wal.Append(&pb.PersistedState{CurrentTerm: int64(i)}, entries)
	}

	totalEntryCount := entriesCount * appendCount

	access := rand.New(rand.NewSource(10)).Perm(totalEntryCount)

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, i := range access {
			e := wal.GetEntry(int64(i))
			if e.Index != int64(i) {
				b.Fatalf("GetEntry error: index=%d, expected=%d", e.Index, i)
			}
		}
	}
}

func BenchmarkWalSequentialScanWithTailCache(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 128*1024, 2*128*1024, zap.NewNop()) // Cache 2 segments.
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Span multiple segments.
	appendCount := 32
	for i := range 32 {
		wal.Append(&pb.PersistedState{CurrentTerm: int64(i)}, entries)
	}

	totalEntryCount := entriesCount * appendCount

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for i := range totalEntryCount {
			e := wal.GetEntry(int64(i))
			if e.Index != int64(i) {
				b.Fatalf("GetEntry error: index=%d, expected=%d", e.Index, i)
			}
		}
	}
}

func BenchmarkWalBatchedSequentialScanWithTailCache(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 128*1024, 2*128*1024, zap.NewNop()) // Cache 2 segments.
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Span multiple segments.
	appendCount := 32
	for i := range 32 {
		wal.Append(&pb.PersistedState{CurrentTerm: int64(i)}, entries)
	}

	totalEntryCount := entriesCount * appendCount

	// Benchmark
	b.ResetTimer()
	for _, batchSize := range []int{1024, 2048, 4096, 8192, 16384} {
		b.Run(fmt.Sprintf("batchSize=%dB", batchSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				for i := 0; i < totalEntryCount; i += batchSize {
					es := wal.GetEntries(int64(i), int64(i+batchSize-1))
					for ei, e := range es {
						if e.Index != int64(i+ei) {
							b.Fatalf("GetEntry error: index=%d, expected=%d", e.Index, i)
						}
					}
				}
			}
		})
	}
}

func BenchmarkRandomScanWithTailCache(b *testing.B) {
	// Setup
	dir := b.TempDir()
	wal, err := graft.OpenWal(dir, 128*1024, 2*128*1024, zap.NewNop()) // Cache 2 segments.
	if err != nil {
		b.Fatalf("OpenWal error: %v", err)
	}

	entriesCount := 1024
	entries := make([]*pb.LogEntry, entriesCount)
	for i := 0; i < entriesCount; i++ {
		entries[i] = &pb.LogEntry{
			Term: int64(i),
			Data: []byte("Hello from the other side"),
			Type: pb.LogEntry_COMMAND,
		}
	}

	// Span multiple segments.
	appendCount := 32
	for i := range 32 {
		wal.Append(&pb.PersistedState{CurrentTerm: int64(i)}, entries)
	}

	totalEntryCount := entriesCount * appendCount

	access := rand.New(rand.NewSource(10)).Perm(totalEntryCount)

	// Benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, i := range access {
			e := wal.GetEntry(int64(i))
			if e.Index != int64(i) {
				b.Fatalf("GetEntry error: index=%d, expected=%d", e.Index, i)
			}
		}
	}
}
