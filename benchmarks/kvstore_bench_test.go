package benchmarks

import (
	"testing"
	"time"
)

func BenchmarkKvStorePut(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 0)
	defer cluster.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := store.Put("k", "v")
		if err != nil {
			b.Fatalf("put error: %v", err)
		}
	}
}

func BenchmarkKvStorePutBatched(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 1*time.Millisecond)
	defer cluster.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := store.Put("k", "v")
		if err != nil {
			b.Fatalf("put error: %v", err)
		}
	}
}

func BenchmarkKvStorePutMultithreaded(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 0)
	defer cluster.Shutdown()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := store.Put("k", "v")
			if err != nil {
				b.Fatalf("put error: %v", err)
			}
		}
	})
}

func BenchmarkKvStorePutMultithreadedWithBatching(b *testing.B) {
	cluster, store := newClusterClient(b, 3, 5*time.Millisecond)
	defer cluster.Shutdown()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, err := store.Put("k", "v")
			if err != nil {
				b.Fatalf("put error: %v", err)
			}
		}
	})
}
