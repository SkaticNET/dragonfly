//go:build unix

package server

import (
	"syscall"
	"testing"
)

func TestNetherNetUDPWireObserverMarksMSGTRUNC(t *testing.T) {
	var events []NetherNetUDPWireObservation
	conn := observeNetherNetUDPConn(flaggedWireUDPConn{n: 1, flags: syscall.MSG_TRUNC}, func(event NetherNetUDPWireObservation) {
		events = append(events, event)
	})
	buffer := make([]byte, 4)
	n, _, flags, _, err := conn.ReadMsgUDP(buffer, nil)
	if n != 1 || flags != syscall.MSG_TRUNC || err != nil {
		t.Fatalf("ReadMsgUDP returned n=%d flags=%d err=%v", n, flags, err)
	}
	if len(events) != 1 || !events[0].PossiblyTruncated || events[0].Direction != NetherNetPacketInbound || len(events[0].Bytes) != n {
		t.Fatalf("MSG_TRUNC observer event = %+v", events)
	}

	var zeroBufferEvents []NetherNetUDPWireObservation
	zeroBufferConn := observeNetherNetUDPConn(flaggedWireUDPConn{n: 0, flags: syscall.MSG_TRUNC}, func(event NetherNetUDPWireObservation) {
		zeroBufferEvents = append(zeroBufferEvents, event)
	})
	n, _, flags, _, err = zeroBufferConn.ReadMsgUDP(nil, nil)
	if n != 0 || flags != syscall.MSG_TRUNC || err != nil {
		t.Fatalf("zero-buffer ReadMsgUDP returned n=%d flags=%d err=%v", n, flags, err)
	}
	if len(zeroBufferEvents) != 1 || !zeroBufferEvents[0].PossiblyTruncated || len(zeroBufferEvents[0].Bytes) != 0 {
		t.Fatalf("explicit zero-buffer MSG_TRUNC was not preserved: %+v", zeroBufferEvents)
	}
}
