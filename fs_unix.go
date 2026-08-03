//go:build !windows

package litex

import (
	"syscall"
)

// getAvailableDiskSpace returns the available disk space in bytes for the given path, or 0 if the
// statistics cannot be read.
func getAvailableDiskSpace(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}

	// Bavail can be negative on some systems; guard against it.
	if stat.Bavail <= 0 || stat.Bsize <= 0 {
		return 0
	}

	// Available blocks * size per block.
	// MUST convert to `uint64` on both sides due to OS differences.
	//nolint:unconvert // It is necessary due to FreeBSD.
	return uint64(stat.Bavail) * uint64(stat.Bsize)
}
