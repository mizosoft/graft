package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/edsrzf/mmap-go"
)

func BenchmarkWriteWithFsync(b *testing.B) {
	// Setup
	rnd := rand.New(rand.NewSource(10))
	sizes := []int{128, 1024, 4096, 16384, 65536} // 128B to 64KB
	datas := make([][]byte, len(sizes))
	for i, size := range sizes {
		datas[i] = make([]byte, size)
		rnd.Read(datas[i])
	}

	// Benchmark
	b.ResetTimer()
	for _, data := range datas {
		b.Run(fmt.Sprintf("dataSize=%dB", len(data)), func(b *testing.B) {
			// Setup
			file, err := os.CreateTemp(b.TempDir(), "fsync-bench")
			if err != nil {
				b.Fatalf("couldn't create file: %v", err)
			}
			defer os.Remove(file.Name())
			defer file.Close()

			// Benchmark
			b.ResetTimer()
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

func BenchmarkWriteWithMsync(b *testing.B) {
	// Setup
	rnd := rand.New(rand.NewSource(10))
	sizes := []int{128, 1024, 4096, 16384, 65536} // 128B to 64KB
	datas := make([][]byte, len(sizes))
	for i, size := range sizes {
		datas[i] = make([]byte, size)
		rnd.Read(datas[i])
	}

	// Benchmark
	b.ResetTimer()
	for _, data := range datas {
		b.Run(fmt.Sprintf("dataSize=%dB", len(data)), func(b *testing.B) {
			file, err := os.CreateTemp(b.TempDir(), "msync-bench")
			if err != nil {
				b.Fatalf("couldn't create file: %v", err)
			}
			defer os.Remove(file.Name())
			defer file.Close()

			if err := file.Truncate(int64(len(data))); err != nil {
				b.Fatalf("truncate error: %v", err)
			}

			mem, err := mmap.Map(file, mmap.RDWR, 0)
			if err != nil {
				b.Fatalf("mmap error: %v", err)
			}
			defer mem.Unmap()

			// Benchmark
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(mem, data)
				if err := mem.Flush(); err != nil {
					b.Fatalf("flush error: %v", err)
				}
			}
		})
	}
}
