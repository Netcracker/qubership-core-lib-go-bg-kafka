package blue_green_kafka

import (
	"context"
	"testing"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWithBlueGreenStatePublisher(t *testing.T) {
	t.Run("should set state publisher when valid", func(t *testing.T) {
		config := &BgKafkaConsumerConfig{}
		mockPublisher := &mockBGStatePublisher{}

		option := WithBlueGreenStatePublisher(mockPublisher)
		option(config)

		publisher, err := config.BlueGreenStatePublisher()
		require.NoError(t, err)
		assert.Equal(t, mockPublisher, publisher)
	})

	t.Run("should panic when state publisher is nil", func(t *testing.T) {
		config := &BgKafkaConsumerConfig{}

		assert.Panics(t, func() {
			option := WithBlueGreenStatePublisher(nil)
			option(config)
		})
	})
}

func TestWithTimeout(t *testing.T) {
	config := &BgKafkaConsumerConfig{}
	timeout := 30 * time.Second

	option := WithTimeout(timeout)
	option(config)

	assert.Equal(t, timeout, config.ReadTimeout())
}

func TestWithConsistencyMode(t *testing.T) {
	config := &BgKafkaConsumerConfig{}
	mode := Eventual

	option := WithConsistencyMode(mode)
	option(config)

	assert.Equal(t, mode, config.ConsistencyMode())
}

func TestWithActiveOffsetSetupStrategy(t *testing.T) {
	config := &BgKafkaConsumerConfig{}
	strategy := StrategyLatest

	option := WithActiveOffsetSetupStrategy(strategy)
	option(config)

	assert.Equal(t, strategy, config.ActiveOffsetSetupStrategy())
}

func TestWithCandidateOffsetSetupStrategy(t *testing.T) {
	config := &BgKafkaConsumerConfig{}
	strategy := StrategyEarliest

	option := WithCandidateOffsetSetupStrategy(strategy)
	option(config)

	assert.Equal(t, strategy, config.CandidateOffsetSetupStrategy())
}

// Mock implementations for testing
type mockBGStatePublisher struct{}

func (m *mockBGStatePublisher) Subscribe(ctx context.Context, callback func(state bgMonitor.BlueGreenState)) {
	// Mock implementation
}

func (m *mockBGStatePublisher) GetState() bgMonitor.BlueGreenState {
	return bgMonitor.BlueGreenState{}
}
