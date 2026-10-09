package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/df-mc/dragonfly/server/world"
	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/login"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type packetHandlerFunc func(packet.Packet, *Session, *world.Tx, Controllable) error

func (f packetHandlerFunc) Handle(pk packet.Packet, s *Session, tx *world.Tx, c Controllable) error {
	return f(pk, s, tx, c)
}

type packetObserverConn struct{ name string }

type packetMetadataConn struct {
	*packetObserverConn
	metadata minecraft.PacketMetadata
}

func (c *packetMetadataConn) LastPacketMetadata() minecraft.PacketMetadata { return c.metadata }

func (*packetObserverConn) Close() error                                               { return nil }
func (*packetObserverConn) IdentityData() login.IdentityData                           { return login.IdentityData{} }
func (*packetObserverConn) ClientData() login.ClientData                               { return login.ClientData{} }
func (*packetObserverConn) ClientCacheEnabled() bool                                   { return false }
func (*packetObserverConn) ChunkRadius() int                                           { return 0 }
func (*packetObserverConn) Latency() time.Duration                                     { return 0 }
func (*packetObserverConn) Flush() error                                               { return nil }
func (c *packetObserverConn) RemoteAddr() net.Addr                                     { return packetObserverAddr(c.name) }
func (*packetObserverConn) ReadPacket() (packet.Packet, error)                         { return nil, nil }
func (*packetObserverConn) WritePacket(packet.Packet) error                            { return nil }
func (*packetObserverConn) StartGameContext(context.Context, minecraft.GameData) error { return nil }

type packetObserverAddr string

func (packetObserverAddr) Network() string  { return "test" }
func (a packetObserverAddr) String() string { return string(a) }

func TestHandlePacketObserverReportsReducedOutcome(t *testing.T) {
	tests := []struct {
		name       string
		pk         packet.Packet
		handle     packetHandler
		status     PacketStatus
		registered bool
	}{
		{name: "unknown", pk: &packet.SetTime{}, status: PacketUnknown},
		{name: "explicit nil handler", pk: &packet.MovePlayer{}, status: PacketUnhandled, registered: true},
		{name: "handled", pk: &packet.SetTime{}, handle: packetHandlerFunc(func(packet.Packet, *Session, *world.Tx, Controllable) error { return nil }), status: PacketHandled, registered: true},
		{name: "handler error", pk: &packet.SetTime{}, handle: packetHandlerFunc(func(packet.Packet, *Session, *world.Tx, Controllable) error {
			return errors.New("sensitive error text")
		}), status: PacketHandlerError, registered: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gotConn Conn
			var got PacketOutcome
			s := &Session{
				conn: &packetObserverConn{name: test.name},
				conf: Config{
					Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
					ObservePacket: func(conn Conn, outcome PacketOutcome) {
						gotConn, got = conn, outcome
					},
				},
				handlers: map[uint32]packetHandler{},
			}
			id := test.pk.ID()
			if test.registered {
				s.handlers[id] = test.handle
			}
			_ = s.handlePacket(test.pk, nil, nil)
			if gotConn != s.conn || got.ID != id || got.Type != fmt.Sprintf("%T", test.pk) || got.Status != test.status || got.Ordinal != 1 {
				t.Fatalf("observer outcome = %#v for connection %v, want id %d type %T status %q ordinal 1", got, gotConn, id, test.pk, test.status)
			}
		})
	}
}

func TestHandlePacketObserverUsesWirePacketMetadata(t *testing.T) {
	conn := &packetMetadataConn{packetObserverConn: &packetObserverConn{name: "wire"}}
	var outcomes []PacketOutcome
	s := &Session{
		conn: conn,
		conf: Config{
			Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
			ObservePacket: func(_ Conn, outcome PacketOutcome) { outcomes = append(outcomes, outcome) },
		},
		handlers: map[uint32]packetHandler{},
	}
	for subIndex := uint32(0); subIndex < 2; subIndex++ {
		conn.metadata = minecraft.PacketMetadata{Direction: minecraft.PacketDirectionInbound, Ordinal: 17, SubIndex: subIndex, PacketID: packet.IDSetTime}
		if err := s.handlePacket(&packet.SetTime{}, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(outcomes) != 2 || outcomes[0].Ordinal != 17 || outcomes[0].SubIndex != 0 || outcomes[1].Ordinal != 17 || outcomes[1].SubIndex != 1 {
		t.Fatalf("wire metadata lost: %+v", outcomes)
	}
}

func TestUnknownPacketLogOmitsPacketFields(t *testing.T) {
	var output bytes.Buffer
	s := &Session{
		conn:     &packetObserverConn{name: "unknown-log"},
		conf:     Config{Log: slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{Level: slog.LevelDebug}))},
		handlers: map[uint32]packetHandler{},
	}
	if err := s.handlePacket(&packet.SetTime{Time: 123456}, nil, nil); err != nil {
		t.Fatal("unknown packet handling failed")
	}
	if bytes.Contains(output.Bytes(), []byte("123456")) || !bytes.Contains(output.Bytes(), []byte("id=")) {
		t.Fatalf("unknown packet log contains packet contents or misses ID: %s", output.String())
	}
}

func TestHandlePacketObserverCorrelatesConcurrentSessions(t *testing.T) {
	type observed struct {
		conn    Conn
		outcome PacketOutcome
	}
	const packetsPerSession = 20
	got := make(chan observed, packetsPerSession*2)
	callback := func(conn Conn, outcome PacketOutcome) { got <- observed{conn: conn, outcome: outcome} }
	newSession := func(name string) *Session {
		return &Session{
			conn:     &packetObserverConn{name: name},
			conf:     Config{Log: slog.New(slog.NewTextHandler(io.Discard, nil)), ObservePacket: callback},
			handlers: map[uint32]packetHandler{packet.IDMovePlayer: nil},
		}
	}
	a, b := newSession("a"), newSession("b")
	var wg sync.WaitGroup
	for _, s := range []*Session{a, b} {
		wg.Add(1)
		go func(s *Session) {
			defer wg.Done()
			for i := 0; i < packetsPerSession; i++ {
				if err := s.handlePacket(&packet.MovePlayer{}, nil, nil); err != nil {
					t.Errorf("handlePacket() error = %v", err)
				}
			}
		}(s)
	}
	wg.Wait()
	close(got)

	ordinals := map[Conn][]uint64{}
	for event := range got {
		ordinals[event.conn] = append(ordinals[event.conn], event.outcome.Ordinal)
		if event.outcome.Status != PacketUnhandled || event.outcome.SubIndex != 0 {
			t.Fatalf("outcome = %#v, want unhandled packet with sub-index 0", event.outcome)
		}
	}
	for _, s := range []*Session{a, b} {
		gotOrdinals := ordinals[s.conn]
		if len(gotOrdinals) != packetsPerSession {
			t.Fatalf("session %v outcome count = %d, want %d", s.conn.RemoteAddr(), len(gotOrdinals), packetsPerSession)
		}
		seen := make(map[uint64]bool, len(gotOrdinals))
		for _, ordinal := range gotOrdinals {
			seen[ordinal] = true
		}
		for ordinal := uint64(1); ordinal <= packetsPerSession; ordinal++ {
			if !seen[ordinal] {
				t.Fatalf("session %v missing ordinal %d: %v", s.conn.RemoteAddr(), ordinal, gotOrdinals)
			}
		}
	}
}
