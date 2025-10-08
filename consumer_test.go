package blue_green_kafka

import (
	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/require"
	"testing"
)

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
