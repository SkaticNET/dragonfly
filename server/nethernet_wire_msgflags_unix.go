//go:build unix

package server

import "syscall"

func netherNetUDPFlagsPossiblyTruncated(flags int) bool {
	return flags&syscall.MSG_TRUNC != 0
}
