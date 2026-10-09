package server

import (
	"net"
	"sync"

	"github.com/pion/transport/v5"
)

type netherNetWireListener struct {
	net.Listener
	observe func(NetherNetSignalingWireObservation)
}

func observeNetherNetSignalingWire(listener net.Listener, observe func(NetherNetSignalingWireObservation)) net.Listener {
	if observe == nil {
		return listener
	}
	return netherNetWireListener{Listener: listener, observe: observe}
}

func (l netherNetWireListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &netherNetWireConn{
		Conn:         conn,
		observe:      l.observe,
		connectionID: NetherNetConnectionID(newNetherNetRequestID()),
		localAddr:    conn.LocalAddr(),
		remoteAddr:   conn.RemoteAddr(),
	}, nil
}

type netherNetWireConn struct {
	net.Conn
	observe      func(NetherNetSignalingWireObservation)
	connectionID NetherNetConnectionID
	localAddr    net.Addr
	remoteAddr   net.Addr
	readMu       sync.Mutex
	writeMu      sync.Mutex
	readOffset   uint64
	writeOffset  uint64
}

func (c *netherNetWireConn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	n, err := c.Conn.Read(p)
	offset := c.readOffset
	if n > 0 {
		c.readOffset += uint64(n)
	}
	c.readMu.Unlock()
	if n > 0 {
		observeNetherNetSignalingWireBytes(c, NetherNetPacketInbound, offset, p[:n])
	}
	return n, err
}

func (c *netherNetWireConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	n, err := c.Conn.Write(p)
	offset := c.writeOffset
	if n > 0 {
		c.writeOffset += uint64(n)
	}
	c.writeMu.Unlock()
	if n > 0 {
		observeNetherNetSignalingWireBytes(c, NetherNetPacketOutbound, offset, p[:n])
	}
	return n, err
}

func observeNetherNetSignalingWireBytes(conn *netherNetWireConn, direction NetherNetPacketDirection, offset uint64, payload []byte) {
	if conn.observe == nil || len(payload) == 0 {
		return
	}
	event := NetherNetSignalingWireObservation{
		ConnectionID: conn.connectionID,
		LocalAddr:    conn.localAddr,
		RemoteAddr:   conn.remoteAddr,
		Direction:    direction,
		ByteOffset:   offset,
		Bytes:        append([]byte(nil), payload...),
	}
	defer func() { _ = recover() }()
	conn.observe(event)
}

type netherNetWireNet struct {
	transport.Net
	observe func(NetherNetUDPWireObservation)
}

func observeNetherNetUDPNet(net transport.Net, observe func(NetherNetUDPWireObservation)) transport.Net {
	if observe == nil {
		return net
	}
	return netherNetWireNet{Net: net, observe: observe}
}

func (n netherNetWireNet) ListenPacket(network, address string) (net.PacketConn, error) {
	conn, err := n.Net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	return netherNetWirePacketConn{PacketConn: conn, observe: n.observe}, nil
}

func (n netherNetWireNet) ListenUDP(network string, address *net.UDPAddr) (transport.UDPConn, error) {
	conn, err := n.Net.ListenUDP(network, address)
	if err != nil {
		return nil, err
	}
	return observeNetherNetUDPConn(conn, n.observe), nil
}

func (n netherNetWireNet) DialUDP(network string, local, remote *net.UDPAddr) (transport.UDPConn, error) {
	conn, err := n.Net.DialUDP(network, local, remote)
	if err != nil {
		return nil, err
	}
	return observeNetherNetUDPConn(conn, n.observe), nil
}

type netherNetWirePacketConn struct {
	net.PacketConn
	observe func(NetherNetUDPWireObservation)
}

func (c netherNetWirePacketConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.PacketConn.ReadFrom(p)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketInbound, c.LocalAddr(), addr, p[:n])
	}
	return n, addr, err
}

func (c netherNetWirePacketConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.PacketConn.WriteTo(p, addr)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketOutbound, c.LocalAddr(), addr, p[:n])
	}
	return n, err
}

type netherNetWireUDPConn struct {
	transport.UDPConn
	observe func(NetherNetUDPWireObservation)
}

func observeNetherNetUDPConn(conn transport.UDPConn, observe func(NetherNetUDPWireObservation)) transport.UDPConn {
	if observe == nil {
		return conn
	}
	return netherNetWireUDPConn{UDPConn: conn, observe: observe}
}

func (c netherNetWireUDPConn) Read(p []byte) (int, error) {
	n, err := c.UDPConn.Read(p)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketInbound, c.LocalAddr(), c.RemoteAddr(), p[:n])
	}
	return n, err
}

func (c netherNetWireUDPConn) ReadFrom(p []byte) (int, net.Addr, error) {
	n, addr, err := c.UDPConn.ReadFrom(p)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketInbound, c.LocalAddr(), addr, p[:n])
	}
	return n, addr, err
}

func (c netherNetWireUDPConn) ReadFromUDP(p []byte) (int, *net.UDPAddr, error) {
	n, addr, err := c.UDPConn.ReadFromUDP(p)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketInbound, c.LocalAddr(), addr, p[:n])
	}
	return n, addr, err
}

func (c netherNetWireUDPConn) ReadMsgUDP(p, oob []byte) (int, int, int, *net.UDPAddr, error) {
	n, oobn, flags, addr, err := c.UDPConn.ReadMsgUDP(p, oob)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketInbound, c.LocalAddr(), addr, p[:n])
	}
	return n, oobn, flags, addr, err
}

func (c netherNetWireUDPConn) Write(p []byte) (int, error) {
	n, err := c.UDPConn.Write(p)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketOutbound, c.LocalAddr(), c.RemoteAddr(), p[:n])
	}
	return n, err
}

func (c netherNetWireUDPConn) WriteTo(p []byte, addr net.Addr) (int, error) {
	n, err := c.UDPConn.WriteTo(p, addr)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketOutbound, c.LocalAddr(), addr, p[:n])
	}
	return n, err
}

func (c netherNetWireUDPConn) WriteToUDP(p []byte, addr *net.UDPAddr) (int, error) {
	n, err := c.UDPConn.WriteToUDP(p, addr)
	if n > 0 {
		observeNetherNetUDPBytes(c.observe, NetherNetPacketOutbound, c.LocalAddr(), addr, p[:n])
	}
	return n, err
}

func (c netherNetWireUDPConn) WriteMsgUDP(p, oob []byte, addr *net.UDPAddr) (int, int, error) {
	n, oobn, err := c.UDPConn.WriteMsgUDP(p, oob, addr)
	if n > 0 {
		remote := net.Addr(addr)
		if addr == nil {
			remote = c.RemoteAddr()
		}
		observeNetherNetUDPBytes(c.observe, NetherNetPacketOutbound, c.LocalAddr(), remote, p[:n])
	}
	return n, oobn, err
}

func observeNetherNetUDPBytes(observe func(NetherNetUDPWireObservation), direction NetherNetPacketDirection, local, remote net.Addr, payload []byte) {
	if observe == nil || len(payload) == 0 {
		return
	}
	event := NetherNetUDPWireObservation{
		Direction:  direction,
		LocalAddr:  local,
		RemoteAddr: remote,
		Bytes:      append([]byte(nil), payload...),
	}
	defer func() { _ = recover() }()
	observe(event)
}
