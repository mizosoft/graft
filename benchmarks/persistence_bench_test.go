package benchmarks

import "testing"

func BenchmarkWalPersistence(b *testing.B) {
	cluster, store := newClusterClientWithWalPersistence(b, 3, 0)
	defer cluster.Shutdown()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := store.Put("k", "v")
		if err != nil {
			b.Fatalf("put error: %v", err)
		}
	}
}

func BenchmarkBadgerPersistence(b *testing.B) {
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
