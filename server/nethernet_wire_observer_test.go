package server

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pion/transport/v5"
	"github.com/pion/transport/v5/stdnet"
)

var errWirePartialIO = errors.New("partial wire test I/O")

type partialWireConn struct{ net.Conn }

func (partialWireConn) Read(p []byte) (int, error) {
	copy(p, "read")
	return 2, errWirePartialIO
}

func (partialWireConn) Write([]byte) (int, error) { return 3, errWirePartialIO }

type partialWireUDPConn struct{ transport.UDPConn }

func (partialWireUDPConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 10}
}
func (partialWireUDPConn) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 11}
}
func (partialWireUDPConn) Read(p []byte) (int, error) {
	copy(p, "read")
	return 2, errWirePartialIO
}
func (partialWireUDPConn) Write([]byte) (int, error) { return 3, errWirePartialIO }

func TestNetherNetWireObserversCapturePartialIOWithErrors(t *testing.T) {
	var signaling []NetherNetSignalingWireObservation
	stream := netherNetWireConn{Conn: partialWireConn{}, observe: func(event NetherNetSignalingWireObservation) {
		event.Bytes = append([]byte(nil), event.Bytes...)
		signaling = append(signaling, event)
	}}
	buffer := make([]byte, 8)
	if n, err := stream.Read(buffer); n != 2 || err != errWirePartialIO || !bytes.Equal(buffer[:n], []byte("re")) {
		t.Fatal("signaling read did not preserve its partial result")
	}
	if n, err := stream.Write([]byte("write")); n != 3 || err != errWirePartialIO {
		t.Fatal("signaling write did not preserve its partial result")
	}
	if len(signaling) != 2 || signaling[0].Direction != NetherNetPacketInbound || !bytes.Equal(signaling[0].Bytes, []byte("re")) || signaling[1].Direction != NetherNetPacketOutbound || !bytes.Equal(signaling[1].Bytes, []byte("wri")) {
		t.Fatal("signaling observer missed partial I/O bytes")
	}

	var datagrams []NetherNetUDPWireObservation
	udp := observeNetherNetUDPConn(partialWireUDPConn{}, func(event NetherNetUDPWireObservation) {
		event.Bytes = append([]byte(nil), event.Bytes...)
		datagrams = append(datagrams, event)
	})
	if n, err := udp.Read(buffer); n != 2 || err != errWirePartialIO || !bytes.Equal(buffer[:n], []byte("re")) {
		t.Fatal("UDP read did not preserve its partial result")
	}
	if n, err := udp.Write([]byte("write")); n != 3 || err != errWirePartialIO {
		t.Fatal("UDP write did not preserve its partial result")
	}
	if len(datagrams) != 2 || datagrams[0].Direction != NetherNetPacketInbound || !bytes.Equal(datagrams[0].Bytes, []byte("re")) || datagrams[1].Direction != NetherNetPacketOutbound || !bytes.Equal(datagrams[1].Bytes, []byte("wri")) {
		t.Fatal("UDP observer missed partial I/O bytes")
	}
}

func TestNetherNetSignalingWireObserverCapturesRawChunkedHTTP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen on loopback")
	}
	defer listener.Close()

	var mu sync.Mutex
	var inbound, outbound []byte
	wired := observeNetherNetSignalingWire(listener, func(event NetherNetSignalingWireObservation) {
		mu.Lock()
		if event.Direction == NetherNetPacketInbound {
			inbound = append(inbound, event.Bytes...)
		} else {
			outbound = append(outbound, event.Bytes...)
		}
		mu.Unlock()
		if len(event.Bytes) > 0 {
			event.Bytes[0] ^= 0xff
		}
		panic("observer panic must not interrupt HTTP")
	})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			http.Error(w, "read body", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(wired) }()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal("connect to local HTTP listener")
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("set client deadline")
	}
	want := []byte("POST /join_offer HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\ntest\r\n0\r\n\r\n")
	if n, err := conn.Write(want); err != nil || n != len(want) {
		t.Fatalf("write local request: n=%d err=%v", n, err)
	}
	if _, err := io.Copy(io.Discard, conn); err != nil {
		t.Fatal("read local HTTP response")
	}
	_ = conn.Close()
	if err := server.Close(); err != nil {
		t.Fatal("close local HTTP server")
	}
	if err := <-serveDone; err != http.ErrServerClosed {
		t.Fatalf("HTTP serve exit = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(inbound, want) {
		t.Fatalf("captured request length = %d, want %d exact stream bytes", len(inbound), len(want))
	}
	if !bytes.HasPrefix(outbound, []byte("HTTP/1.1 204 No Content\r\n")) {
		t.Fatalf("captured response length = %d, want raw HTTP response bytes", len(outbound))
	}
}

func TestNetherNetSignalingWireObserverCorrelatesParallelConnections(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen on loopback")
	}
	defer listener.Close()

	var mu sync.Mutex
	var events []NetherNetSignalingWireObservation
	wired := observeNetherNetSignalingWire(listener, func(event NetherNetSignalingWireObservation) {
		mu.Lock()
		event.Bytes = append([]byte(nil), event.Bytes...)
		events = append(events, event)
		mu.Unlock()
	})

	type streamFlow struct {
		client net.Conn
		server *netherNetWireConn
		in     [][]byte
		out    [][]byte
	}
	flows := make([]streamFlow, 2)
	expected := make(map[NetherNetConnectionID]struct {
		local, remote net.Addr
		in, out       []byte
	})
	for i := range flows {
		client, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal("connect to local signaling listener")
		}
		accepted, err := wired.Accept()
		if err != nil {
			t.Fatal("accept local signaling connection")
		}
		server, ok := accepted.(*netherNetWireConn)
		if !ok || server.connectionID == "" {
			t.Fatal("accepted signaling connection has no opaque ID")
		}
		if err := client.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal("set client deadline")
		}
		if err := server.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal("set server deadline")
		}
		suffix := string(rune('a' + i))
		flows[i] = streamFlow{
			client: client,
			server: server,
			in:     [][]byte{[]byte("client-" + suffix + "-part-1"), []byte("client-" + suffix + "-part-2")},
			out:    [][]byte{[]byte("server-" + suffix + "-part-1"), []byte("server-" + suffix + "-part-2")},
		}
		if _, duplicate := expected[server.connectionID]; duplicate {
			t.Fatal("accepted signaling connections share an ID")
		}
		expected[server.connectionID] = struct {
			local, remote net.Addr
			in, out       []byte
		}{local: server.localAddr, remote: server.remoteAddr, in: bytes.Join(flows[i].in, nil), out: bytes.Join(flows[i].out, nil)}
	}

	writeAll := func(conn net.Conn, payload []byte) error {
		for len(payload) > 0 {
			n, err := conn.Write(payload)
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
			payload = payload[n:]
		}
		return nil
	}
	done := make(chan bool, len(flows))
	for _, flow := range flows {
		go func(flow streamFlow) {
			defer flow.client.Close()
			defer flow.server.Close()
			buffer := make([]byte, 64)
			for i := range flow.in {
				if writeAll(flow.client, flow.in[i]) != nil {
					done <- false
					return
				}
				n, err := io.ReadFull(flow.server, buffer[:len(flow.in[i])])
				if err != nil || !bytes.Equal(buffer[:n], flow.in[i]) || writeAll(flow.server, flow.out[i]) != nil {
					done <- false
					return
				}
				n, err = io.ReadFull(flow.client, buffer[:len(flow.out[i])])
				if err != nil || !bytes.Equal(buffer[:n], flow.out[i]) {
					done <- false
					return
				}
			}
			done <- true
		}(flow)
	}
	for range flows {
		if !<-done {
			t.Fatal("parallel signaling stream exchange failed")
		}
	}

	mu.Lock()
	observed := append([]NetherNetSignalingWireObservation(nil), events...)
	mu.Unlock()
	seen := make(map[NetherNetConnectionID]struct{})
	for _, event := range observed {
		want, ok := expected[event.ConnectionID]
		if !ok || event.LocalAddr == nil || event.RemoteAddr == nil || event.LocalAddr.String() != want.local.String() || event.RemoteAddr.String() != want.remote.String() {
			t.Fatal("signaling observation is missing its connection identity or addresses")
		}
		seen[event.ConnectionID] = struct{}{}
	}
	if len(seen) != len(flows) {
		t.Fatal("signaling observations did not identify both parallel connections")
	}
	for id, want := range expected {
		for direction, payload := range map[NetherNetPacketDirection][]byte{
			NetherNetPacketInbound:  want.in,
			NetherNetPacketOutbound: want.out,
		} {
			fragments := make([]NetherNetSignalingWireObservation, 0)
			for _, event := range observed {
				if event.ConnectionID == id && event.Direction == direction {
					fragments = append(fragments, event)
				}
			}
			sort.Slice(fragments, func(i, j int) bool { return fragments[i].ByteOffset < fragments[j].ByteOffset })
			var got []byte
			for _, fragment := range fragments {
				if fragment.ByteOffset != uint64(len(got)) {
					t.Fatal("signaling byte offsets are not contiguous")
				}
				got = append(got, fragment.Bytes...)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("signaling fragments did not reconstruct the ordered stream")
			}
		}
	}
}

func TestNetherNetUDPWireObserverTapsPacketConn(t *testing.T) {
	base, err := stdnet.NewNet()
	if err != nil {
		t.Fatal("create Pion stdnet")
	}
	var events []NetherNetUDPWireObservation
	tap := observeNetherNetUDPNet(base, func(event NetherNetUDPWireObservation) {
		events = append(events, NetherNetUDPWireObservation{
			Direction: event.Direction,
			LocalAddr: event.LocalAddr, RemoteAddr: event.RemoteAddr,
			Bytes: append([]byte(nil), event.Bytes...),
		})
		if len(event.Bytes) > 0 {
			event.Bytes[0] ^= 0xff
		}
		panic("observer panic must not interrupt UDP")
	})
	pc, err := tap.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen packet through Pion net")
	}
	defer pc.Close()
	peer, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal("listen UDP peer")
	}
	defer peer.Close()
	if err := pc.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("set packet deadline")
	}
	if err := peer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("set peer deadline")
	}

	request, reply := []byte("packet-in"), []byte("packet-out")
	if _, err := peer.WriteTo(request, pc.LocalAddr()); err != nil {
		t.Fatal("send packet to Pion socket")
	}
	buffer := make([]byte, 64)
	n, remote, err := pc.ReadFrom(buffer)
	if err != nil || !bytes.Equal(buffer[:n], request) {
		t.Fatal("read packet from Pion socket")
	}
	if _, err := pc.WriteTo(reply, remote); err != nil {
		t.Fatal("write packet from Pion socket")
	}
	n, _, err = peer.ReadFrom(buffer)
	if err != nil || !bytes.Equal(buffer[:n], reply) {
		t.Fatal("read packet from Pion socket peer")
	}
	if len(events) != 2 || events[0].Direction != NetherNetPacketInbound || !bytes.Equal(events[0].Bytes, request) || events[1].Direction != NetherNetPacketOutbound || !bytes.Equal(events[1].Bytes, reply) {
		t.Fatalf("packet observer directions or copied bytes are incorrect: events=%d", len(events))
	}
}

func TestNetherNetUDPWireObserverTapsUDPConnReadWriteVariants(t *testing.T) {
	base, err := stdnet.NewNet()
	if err != nil {
		t.Fatal("create Pion stdnet")
	}
	var events []NetherNetUDPWireObservation
	observe := func(event NetherNetUDPWireObservation) {
		events = append(events, NetherNetUDPWireObservation{
			Direction: event.Direction,
			LocalAddr: event.LocalAddr, RemoteAddr: event.RemoteAddr,
			Bytes: append([]byte(nil), event.Bytes...),
		})
		if len(event.Bytes) > 0 {
			event.Bytes[0] ^= 0xff
		}
		panic("observer panic must not interrupt UDP")
	}
	tap := observeNetherNetUDPNet(base, observe)
	conn, err := tap.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal("listen UDP through Pion net")
	}
	defer conn.Close()
	peer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal("listen UDP peer")
	}
	defer peer.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("set connection deadline")
	}
	if err := peer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("set peer deadline")
	}
	peerAddr := peer.LocalAddr().(*net.UDPAddr)
	buffer := make([]byte, 64)
	reads := []struct {
		name string
		read func([]byte) (int, net.Addr, error)
	}{
		{name: "Read", read: func(b []byte) (int, net.Addr, error) { n, err := conn.Read(b); return n, peerAddr, err }},
		{name: "ReadFrom", read: conn.ReadFrom},
		{name: "ReadFromUDP", read: func(b []byte) (int, net.Addr, error) { n, addr, err := conn.ReadFromUDP(b); return n, addr, err }},
		{name: "ReadMsgUDP", read: func(b []byte) (int, net.Addr, error) {
			n, _, _, addr, err := conn.ReadMsgUDP(b, nil)
			return n, addr, err
		}},
	}
	for i, test := range reads {
		payload := []byte(test.name)
		if _, err := peer.WriteToUDP(payload, conn.LocalAddr().(*net.UDPAddr)); err != nil {
			t.Fatalf("send for %s: %v", test.name, err)
		}
		n, _, err := test.read(buffer)
		if err != nil || !bytes.Equal(buffer[:n], payload) {
			t.Fatalf("%s returned n=%d err=%v", test.name, n, err)
		}
		if event := events[i]; event.Direction != NetherNetPacketInbound || !bytes.Equal(event.Bytes, payload) {
			t.Fatalf("%s observer did not preserve inbound bytes", test.name)
		}
	}
	writes := []struct {
		name  string
		write func([]byte) (int, error)
	}{
		{name: "WriteTo", write: func(b []byte) (int, error) { return conn.WriteTo(b, peerAddr) }},
		{name: "WriteToUDP", write: func(b []byte) (int, error) { return conn.WriteToUDP(b, peerAddr) }},
		{name: "WriteMsgUDP", write: func(b []byte) (int, error) { n, _, err := conn.WriteMsgUDP(b, nil, peerAddr); return n, err }},
	}
	for i, test := range writes {
		payload := []byte(test.name)
		n, err := test.write(payload)
		if err != nil || n != len(payload) {
			t.Fatalf("%s returned n=%d err=%v", test.name, n, err)
		}
		n, _, err = peer.ReadFromUDP(buffer)
		if err != nil || !bytes.Equal(buffer[:n], payload) {
			t.Fatalf("peer read after %s returned n=%d err=%v", test.name, n, err)
		}
		if event := events[len(reads)+i]; event.Direction != NetherNetPacketOutbound || !bytes.Equal(event.Bytes, payload) {
			t.Fatalf("%s observer did not preserve outbound bytes", test.name)
		}
	}

	connected, err := tap.DialUDP("udp4", nil, peerAddr)
	if err != nil {
		t.Fatal("dial connected Pion UDP socket")
	}
	defer connected.Close()
	if err := connected.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal("set connected deadline")
	}
	connectedIn, connectedOut := []byte("Read"), []byte("Write")
	if _, err := peer.WriteToUDP(connectedIn, connected.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal("send to connected UDP socket")
	}
	n, err := connected.Read(buffer)
	if err != nil || !bytes.Equal(buffer[:n], connectedIn) {
		t.Fatalf("connected Read returned n=%d err=%v", n, err)
	}
	if event := events[len(events)-1]; event.Direction != NetherNetPacketInbound || !bytes.Equal(event.Bytes, connectedIn) {
		t.Fatal("connected Read observer did not preserve inbound bytes")
	}
	if n, err := connected.Write(connectedOut); err != nil || n != len(connectedOut) {
		t.Fatalf("connected Write returned n=%d err=%v", n, err)
	}
	n, _, err = peer.ReadFromUDP(buffer)
	if err != nil || !bytes.Equal(buffer[:n], connectedOut) {
		t.Fatal("peer read after connected Write")
	}
	if event := events[len(events)-1]; event.Direction != NetherNetPacketOutbound || !bytes.Equal(event.Bytes, connectedOut) {
		t.Fatal("connected Write observer did not preserve outbound bytes")
	}
	if len(events) != len(reads)+len(writes)+2 {
		t.Fatalf("UDP callbacks = %d, want one per successful read/write", len(events))
	}
}
