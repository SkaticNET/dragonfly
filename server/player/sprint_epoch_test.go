package player

import (
	"testing"

	"github.com/df-mc/dragonfly/server/world"
)

func TestSprintEpochAdvancesWhenInputObservesNewWorld(t *testing.T) {
	p := &Player{playerData: &playerData{}}
	p.sprintEpoch.Store(1)
	a, b := &world.World{}, &world.World{}

	if got := p.SprintEpochForWorld(a); got != 1 {
		t.Fatalf("initial world epoch = %d, want 1", got)
	}
	if got := p.SprintEpochForWorld(a); got != 1 {
		t.Fatalf("same world epoch = %d, want 1", got)
	}
	if got := p.SprintEpochForWorld(b); got != 2 {
		t.Fatalf("new world epoch = %d, want 2", got)
	}
}

func TestRespawnBoundaryAdvancesEpochEvenWhenTeleportWasCanceled(t *testing.T) {
	t.Run("canceled teleport", func(t *testing.T) {
		p := &Player{playerData: &playerData{}}
		p.sprintEpoch.Store(1)
		previous := p.SprintEpoch()
		p.markRespawnSent(previous)
		if got := p.SprintEpoch(); got != 2 {
			t.Fatalf("epoch after canceled respawn teleport = %d, want 2", got)
		}
	})
	t.Run("successful teleport", func(t *testing.T) {
		p := &Player{playerData: &playerData{}}
		p.sprintEpoch.Store(1)
		previous := p.SprintEpoch()
		p.advanceSprintEpoch() // The successful teleport already advanced it.
		p.markRespawnSent(previous)
		if got := p.SprintEpoch(); got != 2 {
			t.Fatalf("epoch after successful respawn teleport = %d, want 2", got)
		}
	})
}
