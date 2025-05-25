package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
)

func BenchmarkWriteWithFsync(b *testing.B) {
	rnd := rand.New(rand.NewSource(10))
	sizes := []int{128, 1024, 4096, 16384, 65536} // 128B to 64KB
	datas := make([][]byte, len(sizes))
	for i, size := range sizes {
		datas[i] = make([]byte, size)
		rnd.Read(datas[i])
	}

	b.ResetTimer()
	for _, data := range datas {
		b.Run(fmt.Sprintf("%dB", len(data)), func(b *testing.B) {
			file, err := os.CreateTemp(b.TempDir(), "fsync-bench")
			if err != nil {
				b.Fatalf("couldn't create file: %v", err)
			}
			defer os.Remove(file.Name())
			defer file.Close()

			for i := 0; i < b.N; i++ {
				if _, err := file.Write(data); err != nil {
					b.Fatalf("write error: %v", err)
				}
				if err := file.Sync(); err != nil {
					b.Fatalf("sync error: %v", err)
				}
			}
		})
	}
}
