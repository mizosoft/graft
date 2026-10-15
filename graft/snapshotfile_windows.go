package graft

import (
	"os"
	"syscall"
)

// openSnapshotFile is os.Open sharing delete access, which os.Open doesn't, so that a commit can delete a snapshot while
// it's being read, as on Unix.
func openSnapshotFile(name string) (*os.File, error) {
	return openSharingDelete(name, syscall.GENERIC_READ, syscall.OPEN_EXISTING)
}

// createSnapshotFile is os.Create sharing delete access, so that a discarded writer's file can be deleted while a write
// to it is in flight.
func createSnapshotFile(name string) (*os.File, error) {
	return openSharingDelete(name, syscall.GENERIC_READ|syscall.GENERIC_WRITE, syscall.CREATE_ALWAYS)
}

func openSharingDelete(name string, access, createMode uint32) (*os.File, error) {
	namep, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	sharing := uint32(syscall.FILE_SHARE_READ | syscall.FILE_SHARE_WRITE | syscall.FILE_SHARE_DELETE)
	h, err := syscall.CreateFile(namep, access, sharing, nil, createMode, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	return os.NewFile(uintptr(h), name), nil
}
