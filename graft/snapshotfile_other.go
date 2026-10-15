//go:build !windows

package graft

import "os"

// Unix lets an open file be deleted, so snapshot files open as usual. See snapshotfile_windows.go.

func openSnapshotFile(name string) (*os.File, error) {
	return os.Open(name)
}

func createSnapshotFile(name string) (*os.File, error) {
	return os.Create(name)
}
