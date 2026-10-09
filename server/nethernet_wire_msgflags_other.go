//go:build !unix

package server

func netherNetUDPFlagsPossiblyTruncated(int) bool { return false }
