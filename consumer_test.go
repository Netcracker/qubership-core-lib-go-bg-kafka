package blue_green_kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/require"
)

type mockMessage struct {
	headers []Header
	key     []byte
	value   []byte
}

func (m *mockMessage) Topic() string        { return "mock-topic" }
func (m *mockMessage) Offset() int64        { return 0 }
func (m *mockMessage) HighWaterMark() int64 { return 0 }
func (m *mockMessage) Headers() []Header    { return m.headers }
func (m *mockMessage) Key() []byte          { return m.key }
func (m *mockMessage) Value() []byte        { return m.value }
func (m *mockMessage) Partition() int       { return 0 }
func (m *mockMessage) NativeMsg() any       { return nil }

type mockConsumer struct{}

func (m *mockConsumer) Commit(_ context.Context, _ *CommitMarker) error { return nil }
func (m *mockConsumer) Close() error                                    { return nil }
func (m *mockConsumer) ReadMessage(_ context.Context) (Message, error)  { return &mockMessage{}, nil }

type stubBGStatePublisher struct{}

func (m *stubBGStatePublisher) Subscribe(_ context.Context, cb func(bgMonitor.BlueGreenState)) {
	go func() {
		cb(bgMonitor.BlueGreenState{})
	}()
}
func (m *stubBGStatePublisher) GetState() bgMonitor.BlueGreenState {
	return bgMonitor.BlueGreenState{}
}

func TestGetXVersion(t *testing.T) {
	assertions := require.New(t)
	headers := []Header{
		{Key: "x-version", Value: []byte("v1")},
	}
	version := getXVersion(headers)
	assertions.Equal("v1", version)
}

func TestGetXVersionLowerCase(t *testing.T) {
	assertions := require.New(t)
	headers := []Header{
		{Key: "X-Version", Value: []byte("v1")},
	}
	version := getXVersion(headers)
	assertions.Equal("v1", version)
}

func TestGetXVersionNotFound(t *testing.T) {
	assertions := require.New(t)
	headers := []Header{
		{Key: "other-header", Value: []byte("value")},
	}
	version := getXVersion(headers)
	assertions.Equal("", version)
}

func TestGetXVersionEmptyHeaders(t *testing.T) {
	assertions := require.New(t)
	headers := []Header{}
	version := getXVersion(headers)
	assertions.Equal("", version)
}

func TestGetXVersionNilHeaders(t *testing.T) {
	assertions := require.New(t)
	var headers []Header
	version := getXVersion(headers)
	assertions.Equal("", version)
}

func TestCommitMarker_String(t *testing.T) {
	assertions := require.New(t)

	t.Run("should return string representation when not nil", func(t *testing.T) {
		marker := &CommitMarker{
			TopicPartition: TopicPartition{Topic: "test-topic", Partition: 0},
			OffsetAndMeta:  OffsetAndMetadata{Offset: 123},
			Version:        bgMonitor.NamespaceVersion{Version: bgMonitor.NewVersionMust("v1")},
		}

		result := marker.String()
		assertions.Contains(result, "Topic: test-topic")
		assertions.Contains(result, "Partition: 0")
		assertions.Contains(result, "Offset: 123")
		assertions.Contains(result, "Version: v1")
	})

	t.Run("should return 'nil' when marker is nil", func(t *testing.T) {
		var marker *CommitMarker
		result := marker.String()
		assertions.Equal("nil", result)
	})
}

func TestOffsetAndTimestamp_String(t *testing.T) {
	assertions := require.New(t)

	t.Run("should return string representation when not nil", func(t *testing.T) {
		offset := &OffsetAndTimestamp{
			Offset:    456,
			Timestamp: 1234567890,
		}

		result := offset.String()
		assertions.Contains(result, "Offset: 456")
		assertions.Contains(result, "Timestamp: 1234567890")
	})

	t.Run("should return 'nil' when offset is nil", func(t *testing.T) {
		var offset *OffsetAndTimestamp
		result := offset.String()
		assertions.Equal("nil", result)
	})
}

func TestNewConsumer_Success(t *testing.T) {
	ctx := context.Background()
	topic := "test-topic"
	groupIdPrefix := "test-group"
	adminSupplier := func() (NativeAdminAdapter, error) { return nil, nil }
	consumerSupplier := func(groupId string) (Consumer, error) { return &mockConsumer{}, nil }
	mockPublisher := &stubBGStatePublisher{}
	opt := func(cfg *BgKafkaConsumerConfig) {
		cfg.BlueGreenStatePublisher = func() (BGStatePublisher, error) {
			return mockPublisher, nil
		}
	}
	consumer, err := NewConsumer(ctx, topic, groupIdPrefix, adminSupplier, consumerSupplier, opt)
	require.NoError(t, err)
	require.NotNil(t, consumer)
}

func TestNewConsumer_ErrorOnStatePublisher(t *testing.T) {
	ctx := context.Background()
	topic := "test-topic"
	groupIdPrefix := "test-group"
	adminSupplier := func() (NativeAdminAdapter, error) { return nil, nil }
	consumerSupplier := func(groupId string) (Consumer, error) { return &mockConsumer{}, nil }
	opt := func(cfg *BgKafkaConsumerConfig) {
		cfg.BlueGreenStatePublisher = func() (BGStatePublisher, error) {
			return nil, errors.New("fail state publisher")
		}
	}
	consumer, err := NewConsumer(ctx, topic, groupIdPrefix, adminSupplier, consumerSupplier, opt)
	require.Error(t, err)
	require.Nil(t, consumer)
}

func TestBgConsumer_Commit(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version},
	}
	bg := &BgConsumer{
		consumer:      &mockConsumer{},
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
	}
	marker := &CommitMarker{Version: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version}}
	err := bg.Commit(ctx, marker)
	require.NoError(t, err)
}

func TestBgConsumer_Commit_Ignored(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	otherVersion := bgMonitor.NewVersionMust("v2")
	bg := &BgConsumer{
		consumer: &mockConsumer{},
		metrics:  &Metrics{},
		bgStateActive: &bgMonitor.BlueGreenState{
			Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version},
		},
	}
	marker := &CommitMarker{Version: bgMonitor.NamespaceVersion{Namespace: "other", Version: otherVersion}}
	err := bg.Commit(ctx, marker)
	require.NoError(t, err)
}

func TestBgConsumer_Poll_Accepted(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version},
	}
	msg := &mockMessage{
		headers: []Header{{Key: "x-version", Value: []byte("v1")}},
		key:     []byte("k1"),
		value:   []byte("v"),
	}
	mc := &mockConsumerWithMsg{msg: msg}
	bg := &BgConsumer{
		consumer:      mc,
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
		pollMux:       &sync.Mutex{},
		bgStateChange: &atomic.Pointer[bgMonitor.BlueGreenState]{},
	}
	rec, err := bg.Poll(ctx, time.Millisecond*100)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.NotNil(t, rec.Message)
}

func TestBgConsumer_Poll_Rejected(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	otherVersion := bgMonitor.NewVersionMust("v2")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version, State: bgMonitor.StateActive},
		Sibling: &bgMonitor.NamespaceVersion{Namespace: "ns", Version: otherVersion, State: bgMonitor.StateActive},
	}
	msg := &mockMessage{
		headers: []Header{{Key: "x-version", Value: []byte("v2")}},
		key:     []byte("k1"),
		value:   []byte("v"),
	}
	mc := &mockConsumerWithMsg{msg: msg}
	bg := &BgConsumer{
		consumer:      mc,
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
		pollMux:       &sync.Mutex{},
		bgStateChange: &atomic.Pointer[bgMonitor.BlueGreenState]{},
	}
	rec, err := bg.Poll(ctx, time.Millisecond*100)
	require.NoError(t, err)
	require.NotNil(t, rec)
	xVer := getXVersion(msg.Headers())
	accepted, ferr := bg.versionFilter.Test(xVer)
	require.NoError(t, ferr)
	require.False(t, accepted, "Should be false")
}

func TestBgConsumer_Poll_StateChange(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	newVersion := bgMonitor.NewVersionMust("v2")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version, State: bgMonitor.StateActive},
	}
	newState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: newVersion, State: bgMonitor.StateActive},
	}

	mc := &mockConsumerWithState{
		msgs: []Message{&mockMessage{headers: []Header{{Key: "x-version", Value: []byte("v2")}}}},
	}
	topic := "test-topic"
	consumerSupplier := func(groupId string) (Consumer, error) { return mc, nil }
	adminSupplier := func() (NativeAdminAdapter, error) { return &mockAdmin{}, nil }

	bg := &BgConsumer{
		Config: &BgKafkaConsumerConfig{
			ConsumerSupplier:             consumerSupplier,
			AdminSupplier:                adminSupplier,
			Topic:                        func() string { return topic },
			GroupIdPrefix:                func() string { return "test-group" },
			ConsistencyMode:              func() ConsumerConsistencyMode { return Eventual },
			ActiveOffsetSetupStrategy:    func() OffsetSetupStrategy { return StrategyLatest },
			CandidateOffsetSetupStrategy: func() OffsetSetupStrategy { return StrategyLatest },
		},
		consumer:      mc,
		metrics:       NewConsumerMetrics(topic),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
		pollMux:       &sync.Mutex{},
		bgStateChange: &atomic.Pointer[bgMonitor.BlueGreenState]{},
	}

	// Store new state to trigger reinitialization
	bg.bgStateChange.Store(&newState)

	rec, err := bg.Poll(ctx, time.Second)
	require.NoError(t, err)
	require.NotNil(t, rec)
	require.True(t, mc.closed, "Previous consumer should be closed")
}

func TestBgConsumer_Poll_Timeout(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version, State: bgMonitor.StateActive},
	}

	mc := &mockConsumerWithError{err: context.DeadlineExceeded}
	bg := &BgConsumer{
		consumer:      mc,
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
		pollMux:       &sync.Mutex{},
		bgStateChange: &atomic.Pointer[bgMonitor.BlueGreenState]{},
	}

	rec, err := bg.Poll(ctx, time.Millisecond)
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, rec)
}

func TestBgConsumer_Poll_ReadError(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version, State: bgMonitor.StateActive},
	}

	expectedErr := errors.New("read error")
	mc := &mockConsumerWithError{err: expectedErr}
	bg := &BgConsumer{
		consumer:      mc,
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
		pollMux:       &sync.Mutex{},
		bgStateChange: &atomic.Pointer[bgMonitor.BlueGreenState]{},
	}

	rec, err := bg.Poll(ctx, time.Second)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
	require.Nil(t, rec)
}

func TestBgConsumer_Commit_Error(t *testing.T) {
	ctx := context.Background()
	version := bgMonitor.NewVersionMust("v1")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version},
	}
	expectedErr := errors.New("commit error")
	mc := &mockConsumerWithError{err: expectedErr}
	bg := &BgConsumer{
		consumer:      mc,
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
	}
	marker := &CommitMarker{Version: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version}}
	err := bg.Commit(ctx, marker)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}

// Additional mock types for new tests
type mockConsumerWithMsg struct {
	msg Message
}

func (m *mockConsumerWithMsg) ReadMessage(_ context.Context) (Message, error)  { return m.msg, nil }
func (m *mockConsumerWithMsg) Commit(_ context.Context, _ *CommitMarker) error { return nil }
func (m *mockConsumerWithMsg) Close() error                                    { return nil }

type mockConsumerWithState struct {
	msgs   []Message
	closed bool
	idx    int
}

func (m *mockConsumerWithState) ReadMessage(_ context.Context) (Message, error) {
	if m.idx >= len(m.msgs) {
		return nil, errors.New("no more messages")
	}
	msg := m.msgs[m.idx]
	m.idx++
	return msg, nil
}

func (m *mockConsumerWithState) Commit(_ context.Context, _ *CommitMarker) error { return nil }
func (m *mockConsumerWithState) Close() error {
	m.closed = true
	return nil
}

type mockConsumerWithError struct {
	err error
}

func (m *mockConsumerWithError) ReadMessage(_ context.Context) (Message, error)  { return nil, m.err }
func (m *mockConsumerWithError) Commit(_ context.Context, _ *CommitMarker) error { return m.err }
func (m *mockConsumerWithError) Close() error                                    { return nil }

type mockAdmin struct{}

func (m *mockAdmin) ListConsumerGroups(ctx context.Context) ([]ConsumerGroup, error) { return nil, nil }
func (m *mockAdmin) ListConsumerGroupOffsets(ctx context.Context, groupId string) (map[TopicPartition]OffsetAndMetadata, error) {
	return nil, nil
}
func (m *mockAdmin) AlterConsumerGroupOffsets(ctx context.Context, groupId GroupId, offsets map[TopicPartition]OffsetAndMetadata) error {
	return nil
}
func (m *mockAdmin) PartitionsFor(ctx context.Context, topics ...string) ([]PartitionInfo, error) {
	return []PartitionInfo{{Topic: "test-topic", Partition: 0}}, nil
}
func (m *mockAdmin) BeginningOffsets(ctx context.Context, topicPartitions []TopicPartition) (map[TopicPartition]int64, error) {
	return nil, nil
}
func (m *mockAdmin) EndOffsets(ctx context.Context, topicPartitions []TopicPartition) (map[TopicPartition]int64, error) {
	return nil, nil
}
func (m *mockAdmin) OffsetsForTimes(ctx context.Context, times map[TopicPartition]time.Time) (map[TopicPartition]*OffsetAndTimestamp, error) {
	return nil, nil
}

func TestBgConsumer_Stats(t *testing.T) {
	version := bgMonitor.NewVersionMust("v1")
	bgState := bgMonitor.BlueGreenState{
		Current: bgMonitor.NamespaceVersion{Namespace: "ns", Version: version},
	}

	bg := &BgConsumer{
		consumer:      &mockConsumer{},
		metrics:       NewConsumerMetrics("test-topic"),
		bgStateActive: &bgState,
		versionFilter: NewFilter(bgState),
	}

	stats := bg.Stats()
	require.NotNil(t, stats)
}
