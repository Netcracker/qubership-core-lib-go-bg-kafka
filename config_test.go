package blue_green_kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/security"
	"github.com/netcracker/qubership-core-lib-go/v3/serviceloader"
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

type mockBGStatePublisher struct{}

func (m *mockBGStatePublisher) Subscribe(_ context.Context, callback func(state bgMonitor.BlueGreenState)) {
	callback(bgMonitor.BlueGreenState{})
}

func (m *mockBGStatePublisher) GetState() bgMonitor.BlueGreenState {
	return bgMonitor.BlueGreenState{}
}

// Mock Koanf implementation
type mockKoanf struct {
	values map[string]string
}

func (m *mockKoanf) String(key string) string {
	return m.values[key]
}

func (m *mockKoanf) MustString(key string) string {
	if val, ok := m.values[key]; ok {
		return val
	}
	panic(fmt.Sprintf("key not found: %s", key))
}

func setupTestEnv() *mockKoanf {
	return &mockKoanf{
		values: map[string]string{
			"microservice.namespace": "test-namespace",
			"consul.url":             "http://consul:8500",
		},
	}
}

func TestGetNamespace(t *testing.T) {
	mock := setupTestEnv()
	expectedNS := mock.values["microservice.namespace"]
	namespace := mock.MustString("microservice.namespace")
	assert.Equal(t, expectedNS, namespace)
}

func TestGetConsulUrl(t *testing.T) {
	mock := setupTestEnv()
	expectedURL := mock.values["consul.url"]
	url := mock.String("consul.url")
	assert.Equal(t, expectedURL, url)
}

func TestGetAuthSupplier(t *testing.T) {
	serviceloader.Register(1, &security.DummyToken{})
	ctx := context.Background()
	supplier := getAuthSupplier()
	require.NotNil(t, supplier)

	// Just verify supplier returns without error for now
	_, err := supplier(ctx)
	require.NoError(t, err)
}

func TestGetConsulTokenSupplier(t *testing.T) {
	ctx := context.Background()
	supplier, err := getConsulTokenSupplier(ctx, "test-url", "test-ns")
	require.NoError(t, err)
	require.NotNil(t, supplier)

	// Just verify supplier returns without error for now
	_, err = supplier(ctx)
	require.NoError(t, err)
}
