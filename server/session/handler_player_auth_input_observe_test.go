package session

import (
	"log/slog"
	"math"
	"testing"

	"github.com/df-mc/dragonfly/server/block/cube"
	"github.com/df-mc/dragonfly/server/world"
	"github.com/go-gl/mathgl/mgl64"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

type sprintObservationControllable struct {
	Controllable
	moveAccepted bool
}

func (c sprintObservationControllable) Position() mgl64.Vec3              { return mgl64.Vec3{} }
func (c sprintObservationControllable) Rotation() cube.Rotation           { return cube.Rotation{} }
func (c sprintObservationControllable) Move(mgl64.Vec3, float64, float64) {}
func (c sprintObservationControllable) MoveWithResult(mgl64.Vec3, float64, float64) bool {
	return c.moveAccepted
}
func (sprintObservationControllable) StartSprinting() {}
func (sprintObservationControllable) Sneaking() bool  { return false }

func TestPlayerAuthInputCallbackRunsAfterSuccessfulActions(t *testing.T) {
	called := 0
	s := &Session{conf: Config{AfterPlayerAuthInput: func(tx *world.Tx, _ Controllable, got PlayerAuthInputState) {
		called++
		if tx != nil || got.OwnRuntimeID != selfEntityRuntimeID || got.Tick != 42 || !got.Accepted || !got.StartRequested {
			t.Fatalf("callback input = %+v, tx=%v", got, tx)
		}
	}}}
	pk := &packet.PlayerAuthInput{Tick: 42, InputData: protocol.NewInputFlags(packet.InputFlagCount)}
	pk.InputData.Set(packet.InputFlagStartSprinting)
	if err := (PlayerAuthInputHandler{}).Handle(pk, s, nil, sprintObservationControllable{moveAccepted: true}); err != nil {
		t.Fatal(err)
	}
	if called != 1 {
		t.Fatalf("callback count = %d", called)
	}
}

func TestPlayerAuthInputCallbackSkippedOnActionErrorAndNil(t *testing.T) {
	calls := 0
	s := &Session{conf: Config{AfterPlayerAuthInput: func(*world.Tx, Controllable, PlayerAuthInputState) { calls++ }}}
	pk := &packet.PlayerAuthInput{InputData: protocol.NewInputFlags(packet.InputFlagCount)}
	pk.InputData.Set(packet.InputFlagPerformItemInteraction)
	if err := (PlayerAuthInputHandler{}).Handle(pk, s, nil, sprintObservationControllable{moveAccepted: true}); err == nil {
		t.Fatal("missing interaction action accepted")
	}
	if calls != 0 {
		t.Fatalf("callback ran after failed actions: %d", calls)
	}
	s.conf.AfterPlayerAuthInput = nil
	pk = &packet.PlayerAuthInput{InputData: protocol.NewInputFlags(packet.InputFlagCount)}
	if err := (PlayerAuthInputHandler{}).Handle(pk, s, nil, sprintObservationControllable{moveAccepted: true}); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("nil callback invoked: %d", calls)
	}
}

func TestPlayerAuthInputCallbackIgnoresDuplicateAndOutOfOrderTicks(t *testing.T) {
	var got []uint64
	s := &Session{conf: Config{AfterPlayerAuthInput: func(_ *world.Tx, _ Controllable, state PlayerAuthInputState) { got = append(got, state.Tick) }}}
	for _, tick := range []uint64{5, 5, 4, 6} {
		pk := &packet.PlayerAuthInput{Tick: tick, InputData: protocol.NewInputFlags(packet.InputFlagCount), Position: [3]float32{0, 1.62, 0}}
		if err := (PlayerAuthInputHandler{}).Handle(pk, s, nil, sprintObservationControllable{moveAccepted: true}); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 || got[0] != 5 || got[1] != 6 {
		t.Fatalf("callback ticks = %v, want [5 6]", got)
	}
}

func TestPlayerAuthInputMovementAcceptanceRejectsCancelTeleportWaitAndRepairedNonFinite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		prepare      func(*Session, *packet.PlayerAuthInput)
		accepted     bool
		moveAccepted bool
	}{
		{name: "movement cancelled", prepare: func(_ *Session, _ *packet.PlayerAuthInput) {}, accepted: false, moveAccepted: false},
		{name: "teleport wait", prepare: func(s *Session, _ *packet.PlayerAuthInput) { expected := mgl64.Vec3{}; s.teleportPos.Store(&expected) }, accepted: false},
		{name: "repaired non-finite", prepare: func(_ *Session, p *packet.PlayerAuthInput) { p.Position = [3]float32{float32(math.NaN()), 1.62, 0} }, accepted: false},
		{name: "non-finite move vector", prepare: func(_ *Session, p *packet.PlayerAuthInput) { p.MoveVector[0] = float32(math.NaN()) }, accepted: false},
		{name: "unchanged movement while teleport pending", prepare: func(s *Session, _ *packet.PlayerAuthInput) {
			expected := mgl64.Vec3{100, 0, 100}
			s.teleportPos.Store(&expected)
		}, accepted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accepted := !tc.accepted
			s := &Session{conf: Config{Log: slog.New(slog.DiscardHandler), AfterPlayerAuthInput: func(_ *world.Tx, _ Controllable, state PlayerAuthInputState) { accepted = state.Accepted }}}
			pk := &packet.PlayerAuthInput{InputData: protocol.NewInputFlags(packet.InputFlagCount), Position: [3]float32{0, 1.62, 0}}
			tc.prepare(s, pk)
			if err := (PlayerAuthInputHandler{}).Handle(pk, s, nil, sprintObservationControllable{moveAccepted: tc.moveAccepted || tc.name != "movement cancelled"}); err != nil {
				t.Fatal(err)
			}
			if accepted != tc.accepted {
				t.Fatalf("movement accepted = %v, want %v", accepted, tc.accepted)
			}
		})
	}
}
