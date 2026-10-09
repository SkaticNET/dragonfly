package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/df-mc/dragonfly/server/session"
	"github.com/df-mc/go-nethernet"
	"github.com/df-mc/go-nethernet/endpoint"
	"github.com/pion/ice/v4"
	"github.com/pion/transport/v5"
	"github.com/pion/transport/v5/stdnet"
	"github.com/pion/webrtc/v4"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// Listener is a source for connections that may be listened on by a Server using Server.listen. Proxies can use this to
// provide players from a different source.
type Listener interface {
	// Accept blocks until the next connection is established and returns it. An error is returned if the Listener was
	// closed using Close.
	Accept() (session.Conn, error)
	// Disconnect disconnects a connection from the Listener with a reason.
	Disconnect(conn session.Conn, reason string) error
	io.Closer
}

// importPrivateKey reads a PEM file containing a P-384 [ecdsa.PrivateKey] and
// returns it for use by the NetherNet listener.
func importPrivateKey(path string) (*ecdsa.PrivateKey, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err // already wrapped in os.PathError
	}
	block, _ := pem.Decode(b)
	if block == nil {
		return nil, errors.New("invalid PEM block")
	}
	var key *ecdsa.PrivateKey
	switch block.Type {
	case "EC PRIVATE KEY":
		key, err = x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
	case "PRIVATE KEY":
		parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		var ok bool
		key, ok = parsed.(*ecdsa.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("must be *ecdsa.PrivateKey: %T", parsed)
		}
	default:
		return nil, fmt.Errorf("invalid block type: %s", block.Type)
	}
	if key.Curve != elliptic.P384() {
		return nil, fmt.Errorf("private key must use P-384, got %s", key.Curve.Params().Name)
	}
	return key, nil
}

// exportPrivateKey writes a PEM file containing the [ecdsa.PrivateKey].
func exportPrivateKey(path string, key *ecdsa.PrivateKey) error {
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	b := pem.EncodeToMemory(&pem.Block{
		Type:  "EC PRIVATE KEY",
		Bytes: keyBytes,
	})
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("make parent directories: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

// netherNetKey returns the private key identifying the NetherNet listener, imported from path
// if it exists and generated otherwise. Generated keys are persisted to path so that the server
// identity survives restarts; an empty path yields an unsaved temporary key.
func netherNetKey(path string, log *slog.Logger) (*ecdsa.PrivateKey, error) {
	if path == "" {
		key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate key: %w", err)
		}
		log.Warn("Using a temporary private key for the NetherNet listener. Players connecting over plain HTTP may see the TOFU (Trust On First Use) prompt every time the server restarts.")
		return key, nil
	}
	key, err := importPrivateKey(path)
	if os.IsNotExist(err) {
		key, err = ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("generate key: %w", err)
		}
		// Save the generated key so that players are not prompted to trust a
		// new server identity after every restart.
		if err := exportPrivateKey(path, key); err != nil {
			return nil, fmt.Errorf("export private key: %w", err)
		}
		log.Info("Generated a private key for NetherNet listener.", "path", path)
	} else if err != nil {
		return nil, fmt.Errorf("import key file: %w", err)
	}
	return key, nil
}

// rakNetListenerFunc returns a Listener accepting RakNet connections on
// UserConfig.Network.Address. It is the default transport of UserConfig.Config.
func (uc UserConfig) rakNetListenerFunc(conf Config) (Listener, error) {
	l, err := listenerConfig(conf).Listen("raknet", uc.Network.Address)
	if err != nil {
		return nil, fmt.Errorf("create RakNet listener: %w", err)
	}
	conf.Log.Info("RakNet listener running.", "addr", l.Addr())
	return listener{Listener: l}, nil
}

// netherNetListenerFunc returns a Listener accepting NetherNet connections,
// configured from UserConfig.Network.NetherNet.
func (uc UserConfig) netherNetListenerFunc(conf Config) (Listener, error) {
	nc, err := uc.netherNetListenerConfig(conf)
	if err != nil {
		return nil, err
	}
	return nc.Listener(conf)
}

func (uc UserConfig) netherNetListenerConfig(conf Config) (NetherNetConfig, error) {
	nn := uc.Network.NetherNet
	address := nn.Address
	if address == "" {
		address = uc.Network.Address
	}
	key, err := netherNetKey(nn.KeyFile, conf.Log.With("net origin", "nethernet"))
	if err != nil {
		return NetherNetConfig{}, err
	}
	r, err := parsePortRange(nn.UDPPorts)
	if err != nil {
		return NetherNetConfig{}, fmt.Errorf("parse UDP port range: %w", err)
	}
	return NetherNetConfig{Address: address, Key: key, Domain: nn.Domain, UDPPorts: r, Observers: conf.NetherNetObservers}, nil
}

// ListenNetwork returns a Listener accepting connections for conf over any
// [minecraft.Network] at address, for transports beyond the built-in RakNet and
// NetherNet ones. Use it from a Config.Listeners function.
func ListenNetwork(conf Config, network minecraft.Network, address string) (Listener, error) {
	l, err := listenerConfig(conf).ListenNetwork(network, address)
	if err != nil {
		return nil, err
	}
	return listener{Listener: l}, nil
}

// NetherNetConfig may be used to create a NetherNet Listener for a Server, accepting
// connections negotiated over a plaintext HTTP signaling endpoint. Its Listener method
// matches the Config.Listeners function signature.
type NetherNetConfig struct {
	// Address is the TCP address the HTTP signaling endpoint is served on. HTTPS should
	// be terminated by a reverse proxy.
	Address string
	// Key identifies the listener to players connecting over plain HTTP. If nil, a
	// temporary key is generated, causing clients using Trust On First Use (TOFU) to
	// treat every server restart as a new identity.
	Key *ecdsa.PrivateKey
	// Domain is the domain that may be shown in the trust prompt to players connecting
	// over plain HTTP. If empty, "self" is used.
	Domain string
	// HTTPServer optionally configures the server used to serve the signaling endpoint.
	// Its Handler is set by Listener; TLSConfig is ignored because the endpoint is served
	// as plaintext HTTP with HTTPS terminated by a reverse proxy. All other fields may be
	// set freely. If nil, a server with 5s/10s/30s read-header/read/idle timeouts is used.
	HTTPServer *http.Server
	// Credentials optionally supplies ICE (STUN/TURN) servers to the signaling handler.
	// It is required for servers behind NAT or a tunnel, where the reachable game path
	// cannot be established from host candidates alone; the HTTP endpoint only carries
	// signaling. If nil, no STUN/TURN servers are advertised and only host candidates
	// are gathered.
	Credentials func(ctx context.Context) (*nethernet.Credentials, error)
	// UDPPorts is the UDP port range used for player connections. See PortRange
	// for how single ports and zero bounds behave.
	UDPPorts PortRange
	// Observers contains optional hooks for signaling and transport lifecycle
	// metadata. These hooks do not change identity, authentication or port selection.
	Observers NetherNetObservers
}

// NetherNetConnectionID is an opaque, process-local correlation key. It is
// derived from the connection's NetherNet address using a process-random seed.
// Observers should use it only for bounded in-memory correlation.
type NetherNetConnectionID string

// NetherNetRequestID is an opaque, process-local key for one signaling request.
type NetherNetRequestID string

var netherNetConnectionIDSeed = maphash.MakeSeed()

// NetherNetConnectionIDFromAddr returns an opaque ID for a NetherNet address.
// It returns the empty ID for other address types.
func NetherNetConnectionIDFromAddr(addr net.Addr) NetherNetConnectionID {
	nnAddr, ok := addr.(*nethernet.Addr)
	if !ok || nnAddr == nil {
		return ""
	}
	return netherNetConnectionID(nnAddr.NetworkID, nnAddr.ConnectionID)
}

func netherNetConnectionID(networkID string, id uint64) NetherNetConnectionID {
	var hash maphash.Hash
	hash.SetSeed(netherNetConnectionIDSeed)
	_, _ = hash.WriteString(networkID)
	var connectionID [8]byte
	binary.BigEndian.PutUint64(connectionID[:], id)
	_, _ = hash.Write(connectionID[:])
	return NetherNetConnectionID(strconv.FormatUint(hash.Sum64(), 16))
}

type NetherNetSignalingRoute string

const (
	NetherNetRouteJoinPing  NetherNetSignalingRoute = "join_ping"
	NetherNetRouteJoinOffer NetherNetSignalingRoute = "join_offer"
	NetherNetRouteOther     NetherNetSignalingRoute = "other"
)

// NetherNetSignalingObservation contains reduced HTTP metadata and an opaque
// signaling wire connection ID. It never includes paths, addresses, headers,
// bodies or signaling payloads.
type NetherNetSignalingObservation struct {
	WireConnectionID NetherNetConnectionID
	Route            NetherNetSignalingRoute
	Method           string
	StatusCode       int
	RequestLength    int64
	ResponseLength   int
	Duration         time.Duration
}

// NetherNetObservers contains opt-in callbacks for the built-in NetherNet
// listener. DataChannelMessage can contain raw login/gameplay bytes;
// TransportSnapshot can contain SDP, ICE credentials, and peer addresses;
// ObserveRawPacket can contain decoded values and raw packet bytes. Wire
// callbacks receive raw signaling bytes and ICE/WebRTC UDP datagrams, which may
// contain credentials or encrypted player traffic, plus peer addresses. Restrict
// access to them.
// Wire callbacks may run concurrently and must be fast/non-blocking; their byte
// slices are independent copies, and callback panics are ignored.
type NetherNetObservers struct {
	ObserveSignaling            func(NetherNetRequestID, NetherNetSignalingObservation)
	ObserveSignalingWire        func(NetherNetSignalingWireObservation)
	ObserveUDPWire              func(NetherNetUDPWireObservation)
	SignalingNegotiationContext func(context.Context, NetherNetRequestID) (context.Context, context.CancelFunc)
	TransportNegotiationContext func(context.Context) (context.Context, context.CancelFunc)
	ConnContext                 func(context.Context, NetherNetConnectionID) (context.Context, context.CancelFunc)
	ObservePacket               func(NetherNetPacketObservation)
	ObserveRawPacket            func(minecraft.PacketObservation)
	ObserveRemoteDescription    func(NetherNetConnectionID, nethernet.RemoteDescriptionStats)
	ObserveTransportState       func(NetherNetConnectionID, nethernet.TransportLayer, nethernet.TransportState)
	ObserveDataChannelOpen      func(NetherNetConnectionID, nethernet.MessageReliability)
	ObserveDataChannelMessage   func(NetherNetConnectionID, nethernet.DataChannelMessageObservation)
	ObserveTransportSnapshot    func(NetherNetConnectionID, nethernet.TransportSnapshotObservation)
	ObserveConnectionState      func(NetherNetConnectionID, nethernet.ConnectionStateObservation)
	ObserveCorrelationDrop      func(NetherNetConnectionID)
}

type NetherNetPacketDirection string

const (
	NetherNetPacketInbound  NetherNetPacketDirection = "inbound"
	NetherNetPacketOutbound NetherNetPacketDirection = "outbound"
)

// NetherNetSignalingWireObservation contains the n > 0 bytes from one read or
// write on the plaintext signaling TCP stream. ByteOffset is zero-based within
// its direction; TCP read boundaries are not HTTP message boundaries.
type NetherNetSignalingWireObservation struct {
	ConnectionID NetherNetConnectionID
	LocalAddr    net.Addr
	RemoteAddr   net.Addr
	Direction    NetherNetPacketDirection
	ByteOffset   uint64
	Bytes        []byte
}

// NetherNetUDPWireObservation contains the payload bytes from one ICE/WebRTC
// UDP socket read or write, including successful empty datagrams.
type NetherNetUDPWireObservation struct {
	Direction NetherNetPacketDirection
	// PossiblyTruncated is true when an inbound read has no spare buffer capacity or the platform reports truncation.
	PossiblyTruncated bool
	LocalAddr         net.Addr
	RemoteAddr        net.Addr
	Bytes             []byte
}

// NetherNetPacketObservation relates a decoded packet to the remote client on both directions.
type NetherNetPacketObservation struct {
	RemoteID  NetherNetConnectionID
	LocalID   NetherNetConnectionID
	Direction NetherNetPacketDirection
	PacketID  uint32
	Length    int
}

// parsePortRange parses "port" or "min-max" as a PortRange. An empty string
// yields the zero PortRange.
func parsePortRange(s string) (PortRange, error) {
	if s == "" {
		return PortRange{}, nil
	}
	if !strings.Contains(s, "-") {
		v, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return PortRange{}, fmt.Errorf("parse single port: %w", err)
		}
		return PortRange{Min: uint16(v), Max: uint16(v)}, nil
	}
	parts := strings.SplitN(s, "-", 2)
	if len(parts) != 2 {
		return PortRange{}, fmt.Errorf("malformed port range: %s", s)
	}
	minimum, err := strconv.ParseUint(parts[0], 10, 16)
	if err != nil {
		return PortRange{}, fmt.Errorf("parse minimum port: %w", err)
	}
	maximum, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return PortRange{}, fmt.Errorf("parse maximum port: %w", err)
	}
	if minimum > maximum {
		return PortRange{}, fmt.Errorf("invalid port range: %d-%d", minimum, maximum)
	}
	return PortRange{Min: uint16(minimum), Max: uint16(maximum)}, nil
}

// PortRange is an inclusive UDP port range. A single port (Min == Max) is shared
// by all connections through a UDP mux; a wider range can run out of ports under
// load. Zero bounds need no validation: pion/ice's candidate gatherer leaves port
// selection to the operating system when both are zero, and substitutes 1024 for a
// zero Min.
type PortRange struct {
	Min, Max uint16
}

// closeFuncs assembles all Close() functions to be called later.
type closeFuncs []func() error

// deferClose enqueues f to be called later on Close.
func (c *closeFuncs) deferClose(f func() error) {
	*c = append(*c, f)
}

// Close calls all functions registered in c and returns
// all errors combined into one error using [errors.Join].
func (c *closeFuncs) Close() (err error) {
	for _, f := range *c {
		if f == nil {
			continue
		}
		if err2 := f(); err2 != nil {
			err = errors.Join(err, err2)
		}
	}
	return err
}

// Listener returns a Listener accepting NetherNet connections signaled over HTTP.
func (nc NetherNetConfig) Listener(conf Config) (Listener, error) {
	log := conf.Log.With("net origin", "nethernet")
	observers := mergeNetherNetObservers(nc.Observers, conf.NetherNetObservers)
	if nc.Key == nil {
		var err error
		if nc.Key, err = netherNetKey("", log); err != nil {
			return nil, err
		}
	}
	if nc.Domain == "" {
		nc.Domain = "self"
	}

	var deferred closeFuncs
	settingEngine := webrtc.SettingEngine{}
	var wireNet transport.Net
	if observers.ObserveUDPWire != nil {
		baseNet, err := stdnet.NewNet()
		if err != nil {
			return nil, fmt.Errorf("create NetherNet UDP observation net: %w", err)
		}
		wireNet = observeNetherNetUDPNet(baseNet, observers.ObserveUDPWire)
		settingEngine.SetNet(wireNet)
	}
	if ports := nc.UDPPorts; ports.Min != 0 && ports.Min == ports.Max {
		var muxOptions []ice.UDPMuxFromPortOption
		if wireNet != nil {
			muxOptions = append(muxOptions, ice.UDPMuxFromPortWithNet(wireNet))
		}
		mux, err := ice.NewMultiUDPMuxFromPort(int(ports.Min), muxOptions...)
		if err != nil {
			return nil, fmt.Errorf("allocate UDP mux: %w", err)
		}
		deferred.deferClose(mux.Close)
		settingEngine.SetICEUDPMux(mux)
	} else if err := settingEngine.SetEphemeralUDPPortRange(ports.Min, ports.Max); err != nil {
		return nil, fmt.Errorf("configure ephemeral udp port range: %w", err)
	}
	lcfg := netherNetListenConfig(nc, conf, log, webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine)))

	httpLog := conf.Log.With("net origin", "nethernet-http")
	httpServer := nc.HTTPServer
	if httpServer == nil {
		httpServer = &http.Server{
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			IdleTimeout:       30 * time.Second,
		}
	}

	tcp, err := net.Listen("tcp", nc.Address)
	if err != nil {
		_ = deferred.Close()
		return nil, fmt.Errorf("listen NetherNet HTTP: %w", err)
	}
	// Once httpServer.Serve takes ownership of tcp below, closing httpServer is
	// what closes tcp: closing tcp itself as well would race Serve releasing it
	// and report a spurious net.ErrClosed on an otherwise clean shutdown.
	var serving bool
	deferred.deferClose(func() error {
		if serving {
			return httpServer.Close()
		}
		return tcp.Close()
	})

	handlerConfig := endpoint.HandlerConfig{Logger: httpLog, Credentials: nc.Credentials}
	if observers.SignalingNegotiationContext != nil {
		handlerConfig.NegotiationContext = func(parent context.Context) (context.Context, context.CancelFunc) {
			return observers.SignalingNegotiationContext(parent, netherNetRequestIDFromContext(parent))
		}
	}
	handler := handlerConfig.New()
	cfg := observeNetherNetPackets(listenerConfig(conf), handler.NetworkID(), observers.ObservePacket)
	cfg = observeNetherNetRawPackets(cfg, observers.ObserveRawPacket)
	l, err := cfg.ListenNetwork(minecraft.NetherNet{
		Signaling:    handler,
		ListenConfig: lcfg,
	}, handler.NetworkID())
	if err != nil {
		_ = deferred.Close()
		return nil, fmt.Errorf("create NetherNet listener: %w", err)
	}

	var signalingHandler http.Handler = observeNetherNetSignaling(handler, observers)
	httpServer.Handler = logHTTPRequests(httpLog, signalingHandler)
	if observers.ObserveSignalingWire != nil {
		httpServer.ConnContext = observeNetherNetWireConnContext(httpServer.ConnContext)
	}
	serving = true
	go func() {
		err := httpServer.Serve(observeNetherNetSignalingWire(tcp, observers.ObserveSignalingWire))
		if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			conf.Log.Error("NetherNet HTTP listener closed unexpectedly: " + err.Error())
		}
	}()

	conf.Log.Info("NetherNet listener running.", "addr", tcp.Addr())
	return listener{Listener: l, close: deferred.Close}, nil
}

func netherNetListenConfig(nc NetherNetConfig, conf Config, log *slog.Logger, api *webrtc.API) nethernet.ListenConfig {
	observers := mergeNetherNetObservers(nc.Observers, conf.NetherNetObservers)
	ids := newNetherNetObserverIDs(observers.ObserveCorrelationDrop)
	cfg := nethernet.ListenConfig{
		API:            api,
		Log:            log,
		AllowAnonymous: conf.AuthDisabled,
		IssueServerIdentity: func(ctx context.Context) (*nethernet.Identity, error) {
			return nethernet.GenerateServerIdentity(nc.Key, nc.Domain)
		},
		NegotiationContext: observers.TransportNegotiationContext,
	}
	if observers.ConnContext != nil {
		cfg.ConnContext = func(parent context.Context, conn *nethernet.Conn) (context.Context, context.CancelFunc) {
			id := NetherNetConnectionIDFromAddr(conn.RemoteAddr())
			ids.put(conn, id)
			return observers.ConnContext(parent, id)
		}
	}
	if observers.ObserveRemoteDescription != nil || observers.ObserveTransportState != nil || observers.ObserveDataChannelOpen != nil {
		cfg.ObserveRemoteDescription = func(conn *nethernet.Conn, stats nethernet.RemoteDescriptionStats) {
			id := NetherNetConnectionIDFromAddr(conn.RemoteAddr())
			ids.put(conn, id)
			if id != "" && observers.ObserveRemoteDescription != nil {
				observers.ObserveRemoteDescription(id, stats)
			}
		}
	}
	if observers.ConnContext != nil || observers.ObserveRemoteDescription != nil || observers.ObserveTransportState != nil || observers.ObserveDataChannelOpen != nil {
		cfg.ObserveTransportState = func(conn *nethernet.Conn, layer nethernet.TransportLayer, state nethernet.TransportState) {
			id := ids.get(conn)
			if id != "" && observers.ObserveTransportState != nil {
				observers.ObserveTransportState(id, layer, state)
			}
			if state == nethernet.TransportState("closed") || state == nethernet.TransportState("failed") {
				ids.delete(conn)
			}
		}
	}
	if observers.ObserveDataChannelOpen != nil {
		cfg.ObserveDataChannelOpen = func(conn *nethernet.Conn, reliability nethernet.MessageReliability) {
			if id := ids.get(conn); id != "" {
				observers.ObserveDataChannelOpen(id, reliability)
			}
		}
	}
	if observers.ObserveDataChannelMessage != nil {
		cfg.ObserveDataChannelMessage = func(event nethernet.DataChannelMessageObservation) {
			observers.ObserveDataChannelMessage(netherNetConnectionID(event.NetworkID, event.ConnectionID), event)
		}
	}
	if observers.ObserveTransportSnapshot != nil {
		cfg.ObserveTransportSnapshot = func(event nethernet.TransportSnapshotObservation) {
			observers.ObserveTransportSnapshot(netherNetConnectionID(event.NetworkID, event.ConnectionID), event)
		}
	}
	if observers.ObserveConnectionState != nil || observers.ConnContext != nil || observers.ObserveRemoteDescription != nil || observers.ObserveTransportState != nil || observers.ObserveDataChannelOpen != nil {
		cfg.ObserveConnectionState = func(event nethernet.ConnectionStateObservation) {
			id := netherNetConnectionID(event.NetworkID, event.ConnectionID)
			if event.State == nethernet.ConnectionStateClosed {
				ids.deleteID(id)
			}
			if observers.ObserveConnectionState != nil {
				observers.ObserveConnectionState(id, event)
			}
		}
	}
	return cfg
}

func mergeNetherNetObservers(primary, fallback NetherNetObservers) NetherNetObservers {
	if primary.ObserveSignaling == nil {
		primary.ObserveSignaling = fallback.ObserveSignaling
	}
	if primary.ObserveSignalingWire == nil {
		primary.ObserveSignalingWire = fallback.ObserveSignalingWire
	}
	if primary.ObserveUDPWire == nil {
		primary.ObserveUDPWire = fallback.ObserveUDPWire
	}
	if primary.SignalingNegotiationContext == nil {
		primary.SignalingNegotiationContext = fallback.SignalingNegotiationContext
	}
	if primary.TransportNegotiationContext == nil {
		primary.TransportNegotiationContext = fallback.TransportNegotiationContext
	}
	if primary.ConnContext == nil {
		primary.ConnContext = fallback.ConnContext
	}
	if primary.ObservePacket == nil {
		primary.ObservePacket = fallback.ObservePacket
	}
	if primary.ObserveRawPacket == nil {
		primary.ObserveRawPacket = fallback.ObserveRawPacket
	}
	if primary.ObserveRemoteDescription == nil {
		primary.ObserveRemoteDescription = fallback.ObserveRemoteDescription
	}
	if primary.ObserveTransportState == nil {
		primary.ObserveTransportState = fallback.ObserveTransportState
	}
	if primary.ObserveDataChannelOpen == nil {
		primary.ObserveDataChannelOpen = fallback.ObserveDataChannelOpen
	}
	if primary.ObserveDataChannelMessage == nil {
		primary.ObserveDataChannelMessage = fallback.ObserveDataChannelMessage
	}
	if primary.ObserveTransportSnapshot == nil {
		primary.ObserveTransportSnapshot = fallback.ObserveTransportSnapshot
	}
	if primary.ObserveConnectionState == nil {
		primary.ObserveConnectionState = fallback.ObserveConnectionState
	}
	if primary.ObserveCorrelationDrop == nil {
		primary.ObserveCorrelationDrop = fallback.ObserveCorrelationDrop
	}
	return primary
}

type netherNetObserverIDEntry struct {
	id      NetherNetConnectionID
	created time.Time
}

type netherNetObserverIDs struct {
	mu      sync.Mutex
	entries map[*nethernet.Conn]netherNetObserverIDEntry
	onDrop  func(NetherNetConnectionID)
}

func newNetherNetObserverIDs(onDrop func(NetherNetConnectionID)) *netherNetObserverIDs {
	return &netherNetObserverIDs{entries: make(map[*nethernet.Conn]netherNetObserverIDEntry), onDrop: onDrop}
}

func (ids *netherNetObserverIDs) put(conn *nethernet.Conn, id NetherNetConnectionID) {
	if ids == nil || conn == nil || id == "" {
		return
	}
	ids.mu.Lock()
	var dropped NetherNetConnectionID
	if _, ok := ids.entries[conn]; !ok && len(ids.entries) >= 256 {
		var oldest *nethernet.Conn
		var created time.Time
		for candidate, entry := range ids.entries {
			if oldest == nil || entry.created.Before(created) {
				oldest, created = candidate, entry.created
			}
		}
		dropped = ids.entries[oldest].id
		delete(ids.entries, oldest)
	}
	if entry, ok := ids.entries[conn]; ok {
		ids.entries[conn] = netherNetObserverIDEntry{id: id, created: entry.created}
	} else {
		ids.entries[conn] = netherNetObserverIDEntry{id: id, created: time.Now()}
	}
	ids.mu.Unlock()
	if dropped != "" && ids.onDrop != nil {
		ids.onDrop(dropped)
	}
}

func (ids *netherNetObserverIDs) get(conn *nethernet.Conn) NetherNetConnectionID {
	if ids == nil || conn == nil {
		return ""
	}
	ids.mu.Lock()
	defer ids.mu.Unlock()
	entry, ok := ids.entries[conn]
	if !ok {
		return ""
	}
	return entry.id
}

func (ids *netherNetObserverIDs) delete(conn *nethernet.Conn) {
	if ids == nil || conn == nil {
		return
	}
	ids.mu.Lock()
	delete(ids.entries, conn)
	ids.mu.Unlock()
}

func (ids *netherNetObserverIDs) deleteID(id NetherNetConnectionID) {
	if ids == nil || id == "" {
		return
	}
	ids.mu.Lock()
	for conn, entry := range ids.entries {
		if entry.id == id {
			delete(ids.entries, conn)
		}
	}
	ids.mu.Unlock()
}

func observeNetherNetPackets(cfg minecraft.ListenConfig, localNetworkID string, observe func(NetherNetPacketObservation)) minecraft.ListenConfig {
	if observe == nil {
		return cfg
	}
	previous := cfg.PacketFunc
	cfg.PacketFunc = func(header packet.Header, payload []byte, source, destination net.Addr) {
		if previous != nil {
			previous(header, payload, source, destination)
		}
		from, fromOK := source.(*nethernet.Addr)
		to, toOK := destination.(*nethernet.Addr)
		if !fromOK || !toOK || from == nil || to == nil {
			return
		}
		event := NetherNetPacketObservation{PacketID: header.PacketID, Length: len(payload)}
		switch {
		case from.NetworkID == localNetworkID && to.NetworkID != localNetworkID:
			event.Direction, event.RemoteID, event.LocalID = NetherNetPacketOutbound, NetherNetConnectionIDFromAddr(to), NetherNetConnectionIDFromAddr(from)
		case to.NetworkID == localNetworkID && from.NetworkID != localNetworkID:
			event.Direction, event.RemoteID, event.LocalID = NetherNetPacketInbound, NetherNetConnectionIDFromAddr(from), NetherNetConnectionIDFromAddr(to)
		default:
			return
		}
		observe(event)
	}
	return cfg
}

func observeNetherNetRawPackets(cfg minecraft.ListenConfig, observe func(minecraft.PacketObservation)) minecraft.ListenConfig {
	if observe == nil {
		return cfg
	}
	previous := cfg.PacketObserver
	cfg.PacketObserver = func(event minecraft.PacketObservation) {
		if previous != nil {
			previous(event)
		}
		observe(event)
	}
	return cfg
}

type netherNetRequestIDContextKey struct{}

func netherNetRequestIDFromContext(ctx context.Context) NetherNetRequestID {
	id, _ := ctx.Value(netherNetRequestIDContextKey{}).(NetherNetRequestID)
	return id
}

func newNetherNetRequestID() NetherNetRequestID {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return ""
	}
	return NetherNetRequestID(base64.RawURLEncoding.EncodeToString(value[:]))
}

func observeNetherNetSignaling(next http.Handler, observers NetherNetObservers) http.Handler {
	if observers.ObserveSignaling == nil && observers.SignalingNegotiationContext == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := newNetherNetRequestID()
		started := time.Now()
		counted := &netherNetCountingResponseWriter{ResponseWriter: w}
		request := r.WithContext(context.WithValue(r.Context(), netherNetRequestIDContextKey{}, requestID))
		next.ServeHTTP(counted, request)
		status := counted.status
		if status == 0 {
			status = http.StatusOK
		}
		method := r.Method
		switch method {
		case http.MethodGet, http.MethodPost, http.MethodHead, http.MethodOptions:
		default:
			method = "OTHER"
		}
		route := NetherNetRouteOther
		switch {
		case method == http.MethodGet && r.URL.Path == "/v1/join":
			route = NetherNetRouteJoinPing
		case method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/v1/join/"):
			route = NetherNetRouteJoinOffer
		}
		if observers.ObserveSignaling != nil {
			observers.ObserveSignaling(requestID, NetherNetSignalingObservation{WireConnectionID: netherNetWireConnectionIDFromContext(r.Context()), Route: route, Method: method, StatusCode: status, RequestLength: r.ContentLength, ResponseLength: counted.bytes, Duration: time.Since(started)})
		}
	})
}

type netherNetCountingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *netherNetCountingResponseWriter) WriteHeader(status int) {
	if status >= 200 && w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *netherNetCountingResponseWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(data)
	w.bytes += n
	return n, err
}

func (w *netherNetCountingResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// logHTTPRequests logs signaling requests at debug level before passing them to next. The
// endpoint is publicly reachable, so anything louder would let pings and scanners spam the log.
func logHTTPRequests(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Debug("NetherNet HTTP request.")
		next.ServeHTTP(w, r)
	})
}

func listenerConfig(conf Config) minecraft.ListenConfig {
	cfg := minecraft.ListenConfig{
		MaximumPlayers:         conf.MaxPlayers,
		StatusProvider:         conf.StatusProvider,
		AuthenticationDisabled: conf.AuthDisabled,
		ResourcePacks:          conf.Resources,
		TexturePacksRequired:   conf.ResourcesRequired,
		Compression:            conf.Compression,
		Allow:                  conf.Allower.Allow,
	}
	if conf.Log.Enabled(context.Background(), slog.LevelDebug) {
		cfg.ErrorLog = conf.Log.With("net origin", "gophertunnel")
	}
	return cfg
}

// listener is a Listener implementation that wraps around a minecraft.Listener so that it can be listened on by
// Server.
type listener struct {
	*minecraft.Listener
	close func() error // stops the sidecar HTTP signaling server, if any
}

// Accept blocks until the next connection is established and returns it. An error is returned if the Listener was
// closed using Close.
func (l listener) Accept() (session.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return conn.(session.Conn), err
}

// Disconnect disconnects a connection from the Listener with a reason.
func (l listener) Disconnect(conn session.Conn, reason string) error {
	return l.Listener.Disconnect(conn.(*minecraft.Conn), reason)
}

// Close closes the Minecraft listener and any sidecar listener it depends on.
func (l listener) Close() error {
	err := l.Listener.Close()
	if l.close != nil {
		if closeErr := l.close(); err == nil {
			err = closeErr
		}
	}
	return err
}
