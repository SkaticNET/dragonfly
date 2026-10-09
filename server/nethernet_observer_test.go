package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	nethernet "github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/pion/webrtc/v4"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

func TestNetherNetListenerConfigPreservesIdentityAndUDPSelection(t *testing.T) {
	keyFile := filepath.Join(t.TempDir(), "identity.pem")
	user := DefaultConfig()
	user.Network.Address = "127.0.0.1:19132"
	user.Network.NetherNet.Address = "127.0.0.1:19133"
	user.Network.NetherNet.KeyFile = keyFile
	user.Network.NetherNet.Domain = "self"
	user.Network.NetherNet.UDPPorts = "19140-19142"

	conf := Config{Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	first, err := user.netherNetListenerConfig(conf)
	if err != nil {
		t.Fatal("configured listener unavailable")
	}
	second, err := user.netherNetListenerConfig(conf)
	if err != nil {
		t.Fatal("configured listener unavailable on restart")
	}
	if first.Address != "127.0.0.1:19133" || first.Domain != "self" || first.UDPPorts != (PortRange{Min: 19140, Max: 19142}) {
		t.Fatal("observer setup changed configured HTTP or UDP listener selection")
	}
	if first.Key == nil || second.Key == nil || first.Key.PublicKey.X.Cmp(second.Key.PublicKey.X) != 0 || first.Key.PublicKey.Y.Cmp(second.Key.PublicKey.Y) != 0 {
		t.Fatal("configured persistent identity did not survive listener reconstruction")
	}
}

func TestNetherNetListenConfigPassesAllObserverCallbacks(t *testing.T) {
	observers := NetherNetObservers{
		ObserveRemoteDescription: func(NetherNetConnectionID, nethernet.RemoteDescriptionStats) {},
		ObserveTransportState:    func(NetherNetConnectionID, nethernet.TransportLayer, nethernet.TransportState) {},
		ObserveDataChannelOpen:   func(NetherNetConnectionID, nethernet.MessageReliability) {},
		TransportNegotiationContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(parent, 5*time.Second)
		},
		ConnContext: func(parent context.Context, _ NetherNetConnectionID) (context.Context, context.CancelFunc) {
			return context.WithCancel(parent)
		},
	}
	conf := Config{NetherNetObservers: observers}
	nc := NetherNetConfig{Key: nil, Domain: "self"}
	listenConfig := netherNetListenConfig(nc, conf, slog.Default(), nil)
	if listenConfig.ConnContext == nil || listenConfig.NegotiationContext == nil || listenConfig.ObserveRemoteDescription == nil || listenConfig.ObserveTransportState == nil || listenConfig.ObserveDataChannelOpen == nil {
		t.Fatal("NetherNet observer adapters were not installed on the listener")
	}
}

func TestNetherNetRawPacketObserverPreservesPreviousCallback(t *testing.T) {
	var calls []string
	conf := Config{NetherNetObservers: NetherNetObservers{ObserveRawPacket: func(minecraft.PacketObservation) {
		calls = append(calls, "raw")
	}}}
	cfg := minecraft.ListenConfig{}
	cfg.PacketObserver = func(minecraft.PacketObservation) { calls = append(calls, "previous") }
	cfg = observeNetherNetRawPackets(cfg, conf.NetherNetObservers.ObserveRawPacket)
	if cfg.PacketObserver == nil {
		t.Fatal("raw packet observer not installed")
	}
	cfg.PacketObserver(minecraft.PacketObservation{})
	if len(calls) != 2 || calls[0] != "previous" || calls[1] != "raw" {
		t.Fatalf("raw packet callbacks = %v", calls)
	}
}

func TestNetherNetListenConfigInstallsTerminalCleanupForConnectionObservers(t *testing.T) {
	contextObserver := func(context.Context, NetherNetConnectionID) (context.Context, context.CancelFunc) {
		return context.Background(), func() {}
	}
	for _, test := range []struct {
		name      string
		observers NetherNetObservers
	}{
		{name: "connection context", observers: NetherNetObservers{ConnContext: contextObserver}},
		{name: "remote description", observers: NetherNetObservers{ObserveRemoteDescription: func(NetherNetConnectionID, nethernet.RemoteDescriptionStats) {}}},
		{name: "data channel", observers: NetherNetObservers{ObserveDataChannelOpen: func(NetherNetConnectionID, nethernet.MessageReliability) {}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := netherNetListenConfig(NetherNetConfig{}, Config{NetherNetObservers: test.observers}, slog.Default(), nil)
			if config.ObserveTransportState == nil {
				t.Fatal("terminal-state cleanup was not installed without a state observer")
			}
		})
	}
}

func TestNetherNetPacketObserverReceivesReducedMetadata(t *testing.T) {
	var got []NetherNetPacketObservation
	called := 0
	local := &nethernet.Addr{NetworkID: "private-local", ConnectionID: 43, SelectedCandidate: &webrtc.ICECandidate{Address: "192.0.2.11", Port: 45322}}
	remote := &nethernet.Addr{NetworkID: "private-remote", ConnectionID: 42, SelectedCandidate: &webrtc.ICECandidate{Address: "192.0.2.10", Port: 45321}}
	cfg := observeNetherNetPackets(minecraft.ListenConfig{PacketFunc: func(packet.Header, []byte, net.Addr, net.Addr) { called++ }}, "private-local", func(event NetherNetPacketObservation) {
		got = append(got, event)
	})
	cfg.PacketFunc(packet.Header{PacketID: 0x91}, []byte("secret-payload"), remote, local)
	cfg.PacketFunc(packet.Header{PacketID: 0x92}, []byte("reply"), local, remote)
	if called != 2 || len(got) != 2 || got[0].Direction != NetherNetPacketInbound || got[1].Direction != NetherNetPacketOutbound || got[0].RemoteID != got[1].RemoteID || got[0].RemoteID != NetherNetConnectionIDFromAddr(remote) || got[0].LocalID != NetherNetConnectionIDFromAddr(local) || got[0].PacketID != 0x91 || got[0].Length != len("secret-payload") || got[1].PacketID != 0x92 || got[1].Length != len("reply") {
		t.Fatalf("NetherNet packet observer lost direction or client correlation: %+v", got)
	}
	for _, forbidden := range []string{"private-source", "private-destination", "192.0.2.10", "192.0.2.11"} {
		if strings.Contains(string(got[0].RemoteID), forbidden) || strings.Contains(string(got[0].LocalID), forbidden) {
			t.Fatalf("NetherNet connection ID exposed address metadata: %q", forbidden)
		}
	}
}

func TestNetherNetTransportObserversKeepOneOpaqueConnectionID(t *testing.T) {
	var ids []NetherNetConnectionID
	config := netherNetListenConfig(NetherNetConfig{}, Config{NetherNetObservers: NetherNetObservers{
		ObserveDataChannelMessage: func(id NetherNetConnectionID, _ nethernet.DataChannelMessageObservation) { ids = append(ids, id) },
		ObserveTransportSnapshot:  func(id NetherNetConnectionID, _ nethernet.TransportSnapshotObservation) { ids = append(ids, id) },
		ObserveConnectionState:    func(id NetherNetConnectionID, _ nethernet.ConnectionStateObservation) { ids = append(ids, id) },
	}}, slog.Default(), nil)
	config.ObserveDataChannelMessage(nethernet.DataChannelMessageObservation{ConnectionID: 42, NetworkID: "client"})
	config.ObserveTransportSnapshot(nethernet.TransportSnapshotObservation{ConnectionID: 42, NetworkID: "client"})
	config.ObserveConnectionState(nethernet.ConnectionStateObservation{ConnectionID: 42, NetworkID: "client", State: nethernet.ConnectionStateClosed})
	want := netherNetConnectionID("client", 42)
	if len(ids) != 3 || ids[0] != want || ids[1] != want || ids[2] != want || want == netherNetConnectionID("other-client", 42) {
		t.Fatalf("transport callbacks lost per-network correlation: %v", ids)
	}
}

func TestNetherNetObserverIDMapBoundsAndReportsEviction(t *testing.T) {
	var dropped NetherNetConnectionID
	ids := newNetherNetObserverIDs(func(id NetherNetConnectionID) { dropped = id })
	conns := make([]*nethernet.Conn, 257)
	for i := range conns {
		conns[i] = new(nethernet.Conn)
		ids.put(conns[i], NetherNetConnectionID(strconv.Itoa(i)))
	}
	if len(ids.entries) != 256 || dropped != "0" || ids.get(conns[0]) != "" || ids.get(conns[256]) == "" {
		t.Fatal("bounded correlation eviction was not reported or did not release the old connection")
	}
}

func TestBuiltInNetherNetListenerInvokesObserversDuringNegotiation(t *testing.T) {
	var mu sync.Mutex
	called := map[string]bool{}
	var signalingID, negotiationID NetherNetRequestID
	events := make(chan struct{}, 32)
	mark := func(name string) {
		mu.Lock()
		called[name] = true
		mu.Unlock()
		select {
		case events <- struct{}{}:
		default:
		}
	}
	observers := NetherNetObservers{
		ObserveSignaling: func(id NetherNetRequestID, observation NetherNetSignalingObservation) {
			if observation.Route == NetherNetRouteJoinOffer && observation.Method == "POST" && observation.StatusCode >= 200 {
				mu.Lock()
				signalingID = id
				mu.Unlock()
				mark("signaling")
			}
		},
		SignalingNegotiationContext: func(parent context.Context, id NetherNetRequestID) (context.Context, context.CancelFunc) {
			mu.Lock()
			negotiationID = id
			mu.Unlock()
			mark("signaling-negotiation-start")
			ctx, cancel := context.WithTimeout(parent, 15*time.Second)
			return ctx, func() { cancel(); mark("signaling-negotiation-closed") }
		},
		TransportNegotiationContext: func(parent context.Context) (context.Context, context.CancelFunc) {
			mark("transport-negotiation-start")
			ctx, cancel := context.WithTimeout(parent, 5*time.Second)
			return ctx, func() { cancel(); mark("transport-negotiation-closed") }
		},
		ConnContext: func(parent context.Context, id NetherNetConnectionID) (context.Context, context.CancelFunc) {
			if id != "" {
				mark("transport-context-start")
			}
			ctx, cancel := context.WithTimeout(parent, 5*time.Second)
			return ctx, func() { cancel(); mark("transport-context-closed") }
		},
		ObserveRemoteDescription: func(id NetherNetConnectionID, _ nethernet.RemoteDescriptionStats) {
			if id != "" {
				mark("remote-description")
			}
		},
		ObserveTransportState: func(id NetherNetConnectionID, _ nethernet.TransportLayer, _ nethernet.TransportState) {
			if id != "" {
				mark("transport-state")
			}
		},
		ObserveDataChannelOpen: func(id NetherNetConnectionID, _ nethernet.MessageReliability) {
			if id != "" {
				mark("data-channel")
			}
		},
	}

	user := DefaultConfig()
	user.Network.Transport = []string{"nethernet"}
	user.Network.NetherNet.KeyFile = filepath.Join(t.TempDir(), "identity.pem")
	user.Network.NetherNet.UDPPorts = "0"
	conf := Config{
		Log:                slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxPlayers:         10,
		AuthDisabled:       true,
		Allower:            allower{},
		NetherNetObservers: observers,
	}
	var address string
	var l Listener
	for attempt := 0; attempt < 5; attempt++ {
		tcp, listenErr := net.Listen("tcp", "127.0.0.1:0")
		if listenErr != nil {
			t.Fatal("reserve local signaling address")
		}
		address = tcp.Addr().String()
		if err := tcp.Close(); err != nil {
			t.Fatal("release local signaling address")
		}
		user.Network.NetherNet.Address = address
		listeners, err := user.transportListeners()
		if err != nil || len(listeners) != 1 {
			t.Fatal("built-in transport selection failed")
		}
		l, err = listeners[0](conf)
		if err == nil {
			break
		}
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatal("built-in NetherNet listener failed to start")
		}
	}
	if l == nil {
		t.Fatal("could not reserve a local signaling address after bounded retries")
	}
	defer l.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	client := endpoint.ClientConfig{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}.New()
	conn, err := (nethernet.Dialer{DisableTrickleICE: true}).DialContext(ctx, "http://"+address, client)
	if err != nil {
		t.Fatal("local NetherNet negotiation failed")
	}
	defer conn.Close()

	want := []string{
		"signaling", "remote-description", "transport-state", "data-channel",
		"signaling-negotiation-start", "signaling-negotiation-closed",
		"transport-negotiation-start", "transport-negotiation-closed",
		"transport-context-start", "transport-context-closed",
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		mu.Lock()
		missing := ""
		for _, event := range want {
			if !called[event] {
				missing = event
				break
			}
		}
		mu.Unlock()
		if missing == "" {
			break
		}
		select {
		case <-events:
		case <-deadline.C:
			t.Fatalf("built-in listener did not invoke %s observer", missing)
		}
	}
	mu.Lock()
	idsMatch := signalingID != "" && signalingID == negotiationID
	mu.Unlock()
	if !idsMatch {
		t.Fatal("signaling response and negotiation did not share an opaque request ID")
	}
}
