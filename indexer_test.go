package blue_green_kafka

import (
	"context"
	"fmt"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestSimilarNamesOfGroups(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
	nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).
		Return([]ConsumerGroup{
			{GroupId: "test"},
			{GroupId: "test-v1-a_i-2023-07-07_10-30-00"},
			{GroupId: "test-with-suffix"},
			{GroupId: "test-with-suffix-v1-a_i-2023-07-07_10-30-00"},
		}, nil)
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}

	nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").
		Return(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 1}}, nil)
	nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test-v1-a_i-2023-07-07_10-30-00").
		Return(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 2}}, nil)

	indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
	assertions.NoError(err)

	current, err := ParseGroupId("test-v1-a_c-2023-07-07_11-30-00")
	assertions.NoError(err)
	previousStateOffset := indexer.findPreviousStateOffset(ctx, current)
	assertions.Equal(1, len(previousStateOffset))
	assertions.Equal("test-v1-a_i-2023-07-07_10-30-00", previousStateOffset[0].groupId.String())
	assertions.Equal("test", previousStateOffset[0].groupId.GetGroupIdPrefix())
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 2}}, previousStateOffset[0].offset)
}

func TestMigrationDoesNotExist(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
	nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).
		Return([]ConsumerGroup{
			{GroupId: "test"},
			{GroupId: "test-v1v2a1725365278"},
		}, nil)
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}

	nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").
		Return(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 1}}, nil)
	nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test-v1v2a1725365278").
		Return(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 2}}, nil)

	nativeAdminAdapter.EXPECT().AlterConsumerGroupOffsets(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, groupIdPrefix GroupId, proposedOffsets map[TopicPartition]OffsetAndMetadata) error {
			if !strings.HasPrefix(groupIdPrefix.String(), "test-v1v2M") {
				return fmt.Errorf("invalid group: %s", groupIdPrefix.String())
			}
			return nil
		})

	indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
	assertions.NoError(err)

	assertions.NoError(err)
	err = indexer.createMigrationDoneFromBg1MarkerGroup(ctx)
	assertions.NoError(err)
}

func TestOffsetIndexer_committedOffsets(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	t.Run("should return nil when group does not exist", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		groupId := MustParseGroupId("nonexistent-group")
		assertions.Nil(indexer.committedOffsets(groupId))
	})

	t.Run("should return the group's offsets, even searched as a distinct GroupId instance", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 42}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		search, err := ParseGroupId("test") // freshly allocated, distinct pointer from the one indexed above
		assertions.NoError(err)
		result := indexer.committedOffsets(search)
		assertions.Equal(map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 42}}, result)
	})

	t.Run("should exclude partitions reported with a negative (uncommitted) sentinel offset", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").Return(
			map[TopicPartition]OffsetAndMetadata{
				{Partition: 0, Topic: "test-topic"}: {Offset: 42},
				{Partition: 1, Topic: "test-topic"}: {Offset: -1}, // no committed offset
			}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		result := indexer.committedOffsets(MustParseGroupId("test"))
		assertions.Equal(map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 42}}, result)
	})

	t.Run("should return nil when the group's committed offsets are for a different topic", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "other-topic"}: {Offset: 42}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		search := MustParseGroupId("test")
		assertions.Nil(indexer.committedOffsets(search))
	})
}

func TestOffsetIndexer_findPreviousStateOffset_excludesCurrentGroup(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
	nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
		{GroupId: "test"},
	}, nil)
	nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").Return(
		map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 42}}, nil)

	indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
	assertions.NoError(err)

	current, err := ParseGroupId("test")
	assertions.NoError(err)
	result := indexer.findPreviousStateOffset(ctx, current)
	assertions.Empty(result)
}

func TestOffsetIndexer_bg1VersionsExist(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	t.Run("should return true when bg1 versions exist", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test-v1v2a1725365278"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test-v1v2a1725365278").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 1}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		result := indexer.bg1VersionsExist()
		assertions.True(result)
	})

	t.Run("should return false when bg1 versions do not exist", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test-v1-a_i-2023-07-07_10-30-00"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test-v1-a_i-2023-07-07_10-30-00").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 1}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		result := indexer.bg1VersionsExist()
		assertions.False(result)
	})
}

func TestOffsetIndexer_bg1VersionsMigrated(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	t.Run("should return true when bg1 versions are migrated", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test-v1v2M1759926089"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test-v1v2M1759926089").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 1}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		result := indexer.bg1VersionsMigrated()
		assertions.True(result)
	})

	t.Run("should return false when bg1 versions are not migrated", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test-v1v2a1725365278"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test-v1v2a1725365278").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 1}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		result := indexer.bg1VersionsMigrated()
		assertions.False(result)
	})
}
