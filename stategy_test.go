package blue_green_kafka

import (
	"testing"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/assert"
)

func TestRewind(t *testing.T) {
	t.Run("should create rewind strategy with correct shift", func(t *testing.T) {
		rewindInterval := 5 * time.Minute
		strategy := Rewind(rewindInterval)

		assert.Equal(t, rewindInterval, strategy.shift)
		assert.Equal(t, "rewind on 5m0s", strategy.description)
	})

	t.Run("should create rewind strategy with zero duration", func(t *testing.T) {
		rewindInterval := 0 * time.Second
		strategy := Rewind(rewindInterval)

		assert.Equal(t, rewindInterval, strategy.shift)
		assert.Equal(t, "rewind on 0s", strategy.description)
	})

	t.Run("should create rewind strategy with negative duration", func(t *testing.T) {
		rewindInterval := -1 * time.Hour
		strategy := Rewind(rewindInterval)

		assert.Equal(t, rewindInterval, strategy.shift)
		assert.Equal(t, "rewind on -1h0m0s", strategy.description)
	})
}

func TestOffsetSetupStrategy_getShift(t *testing.T) {
	t.Run("should return correct shift duration", func(t *testing.T) {
		shift := 10 * time.Minute
		strategy := OffsetSetupStrategy{
			shift:       shift,
			description: "test strategy",
		}

		assert.Equal(t, shift, strategy.getShift())
	})

	t.Run("should return zero duration for uninitialized strategy", func(t *testing.T) {
		strategy := OffsetSetupStrategy{}

		assert.Equal(t, time.Duration(0), strategy.getShift())
	})
}

func TestCompareGroupStates(t *testing.T) {
	t.Run("should return true for matching versioned group states", func(t *testing.T) {
		group := &VersionedGroupId{
			State:        bgMonitor.StateActive,
			SiblingState: bgMonitor.StateCandidate,
		}

		result := compareGroupStates(group, bgMonitor.StateActive, bgMonitor.StateCandidate)

		assert.True(t, result)
	})

	t.Run("should return false for non-matching versioned group states", func(t *testing.T) {
		group := &VersionedGroupId{
			State:        bgMonitor.StateActive,
			SiblingState: bgMonitor.StateCandidate,
		}

		result := compareGroupStates(group, bgMonitor.StateCandidate, bgMonitor.StateActive)

		assert.False(t, result)
	})

	t.Run("should return false for non-versioned group", func(t *testing.T) {
		group := MustParseGroupId("plain-group")

		result := compareGroupStates(group, bgMonitor.StateActive, bgMonitor.StateCandidate)

		assert.False(t, result)
	})

	t.Run("should return false for nil group", func(t *testing.T) {
		result := compareGroupStates(nil, bgMonitor.StateActive, bgMonitor.StateCandidate)

		assert.False(t, result)
	})
}
