package blue_green_kafka

import (
	"context"
	"errors"
	"testing"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/require"
)

type mockMessage struct{}

func (m *mockMessage) Topic() string        { return "mock-topic" }
func (m *mockMessage) Offset() int64        { return 0 }
func (m *mockMessage) HighWaterMark() int64 { return 0 }
func (m *mockMessage) Headers() []Header    { return nil }
func (m *mockMessage) Key() []byte          { return nil }
func (m *mockMessage) Value() []byte        { return nil }
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
