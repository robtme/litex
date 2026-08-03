//go:build windows

package litex

import (
	"syscall"
	"unsafe"
)

// getAvailableDiskSpace returns the available disk space in bytes for the given path, or 0 if the
// statistics cannot be read.
func getAvailableDiskSpace(path string) uint64 {
	kernel32, err := syscall.LoadDLL("kernel32.dll")
	if err != nil {
		return 0
	}
	defer kernel32.Release()

	GetDiskFreeSpaceEx, err := kernel32.FindProc("GetDiskFreeSpaceExW")
	if err != nil {
		return 0
	}

	var freeBytesAvailable, total, totalFree uint64

	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return 0
	}

	r1, _, err := GetDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		uintptr(unsafe.Pointer(&total)),
		uintptr(unsafe.Pointer(&totalFree)),
	)

	if r1 == 0 {
		return 0
	}

	return freeBytesAvailable
}
