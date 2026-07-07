package blue_green_kafka

import (
	"context"
	"testing"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultTrackingVersionFilterActive(t *testing.T) {
	assertions := require.New(t)
	statePublisher := &testBGStatePublisher{callbacks: []func(state bgMonitor.BlueGreenState){}}
	filter := NewTrackingVersionFilter(context.Background(), statePublisher)

	statePublisher.setState(bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "test-namespace-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})

	assertions.True(filter.Test("v1"))
	assertions.Equal("true", filter.String())
}

func TestNewDefaultTrackingVersionFilterActiveAndCandidate(t *testing.T) {
	assertions := require.New(t)

	state := bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "test-namespace-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		Sibling:    &bgMonitor.NamespaceVersion{Namespace: "test-namespace-2", Version: bgMonitor.NewVersionMust("v2"), State: bgMonitor.StateCandidate},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	statePublisher := &testBGStatePublisher{state: state, callbacks: []func(state bgMonitor.BlueGreenState){}}
	filter := NewTrackingVersionFilter(context.Background(), statePublisher)

	assertions.True(filter.Test("v1"))
	assertions.False(filter.Test("v2"))
	assertions.Equal("!v2", filter.String())
}

func TestNewVersionNameFilterNoSibling(t *testing.T) {
	assertions := require.New(t)
	for _, state := range []bgMonitor.State{bgMonitor.StateActive, bgMonitor.StateCandidate, bgMonitor.StateLegacy, bgMonitor.StateIdle} {
		filter := NewVersionNameFilter(bgMonitor.BlueGreenState{
			Current:    bgMonitor.NamespaceVersion{Namespace: "ns-1", Version: bgMonitor.NewVersionMust("v1"), State: state},
			UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
		})
		assertions.Equal("true", filter.Presentation)
		accepted, err := filter.Test("any-name")
		assertions.NoError(err)
		assertions.True(accepted)
	}
}

func TestNewVersionNameFilterActiveIdle(t *testing.T) {
	assertions := require.New(t)
	filter := NewVersionNameFilter(bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "ns-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		Sibling:    &bgMonitor.NamespaceVersion{Namespace: "ns-2", State: bgMonitor.StateIdle},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	assertions.Equal("true", filter.Presentation)
	accepted, _ := filter.Test("CANDIDATE")
	assertions.True(accepted)
}

func TestNewVersionNameFilterActiveCandidate(t *testing.T) {
	assertions := require.New(t)
	filter := NewVersionNameFilter(bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "ns-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		Sibling:    &bgMonitor.NamespaceVersion{Namespace: "ns-2", Version: bgMonitor.NewVersionMust("v2"), State: bgMonitor.StateCandidate},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	assertions.Equal("!CANDIDATE", filter.Presentation)
	assertReject(assertions, filter, "CANDIDATE")
	assertReject(assertions, filter, "candidate")
	assertAccept(assertions, filter, "ACTIVE")
	assertAccept(assertions, filter, "LEGACY")
}

func TestNewVersionNameFilterCandidateActive(t *testing.T) {
	assertions := require.New(t)
	filter := NewVersionNameFilter(bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "ns-1", Version: bgMonitor.NewVersionMust("v2"), State: bgMonitor.StateCandidate},
		Sibling:    &bgMonitor.NamespaceVersion{Namespace: "ns-2", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateActive},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	assertions.Equal("CANDIDATE", filter.Presentation)
	assertAccept(assertions, filter, "CANDIDATE")
	assertAccept(assertions, filter, "candidate")
	assertReject(assertions, filter, "ACTIVE")
	assertReject(assertions, filter, "LEGACY")
	assertReject(assertions, filter, "")
}

func TestNewVersionNameFilterLegacyActive(t *testing.T) {
	assertions := require.New(t)
	filter := NewVersionNameFilter(bgMonitor.BlueGreenState{
		Current:    bgMonitor.NamespaceVersion{Namespace: "ns-1", Version: bgMonitor.NewVersionMust("v1"), State: bgMonitor.StateLegacy},
		Sibling:    &bgMonitor.NamespaceVersion{Namespace: "ns-2", Version: bgMonitor.NewVersionMust("v2"), State: bgMonitor.StateActive},
		UpdateTime: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	assertions.Equal("LEGACY", filter.Presentation)
	assertAccept(assertions, filter, "LEGACY")
	assertReject(assertions, filter, "ACTIVE")
	assertReject(assertions, filter, "CANDIDATE")
}

func assertAccept(assertions *require.Assertions, filter *Filter, name string) {
	accepted, err := filter.Test(name)
	assertions.NoError(err)
	assertions.True(accepted)
}

func assertReject(assertions *require.Assertions, filter *Filter, name string) {
	accepted, err := filter.Test(name)
	assertions.NoError(err)
	assertions.False(accepted)
}

type testBGStatePublisher struct {
	state     bgMonitor.BlueGreenState
	callbacks []func(state bgMonitor.BlueGreenState)
}

func (t *testBGStatePublisher) Subscribe(ctx context.Context, callback func(state bgMonitor.BlueGreenState)) {
	t.callbacks = append(t.callbacks, callback)
}

func (t *testBGStatePublisher) GetState() bgMonitor.BlueGreenState {
	return t.state
}

func (t *testBGStatePublisher) setState(state bgMonitor.BlueGreenState) {
	t.state = state
	for _, callback := range t.callbacks {
		callback(state)
	}
}
