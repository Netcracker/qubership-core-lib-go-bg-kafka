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

func TestOffsetIndexer_exists(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	t.Run("should return false when group does not exist", func(t *testing.T) {
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		groupId := MustParseGroupId("nonexistent-group")
		result := indexer.exists(groupId, []TopicPartition{{Partition: 0, Topic: "test-topic"}})
		assertions.False(result)
	})

	t.Run("should return true when group exists, even parsed as a distinct GroupId instance", func(t *testing.T) {
		// Regression test: GroupId implementations are pointers allocated fresh by
		// ParseGroupId, so exists() must compare by content (GroupId.String()), not by
		// the GroupId interface value's address, or an existing group would never be
		// recognized as already indexed.
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
		result := indexer.exists(search, []TopicPartition{{Partition: 0, Topic: "test-topic"}})
		assertions.True(result)
	})

	t.Run("should return false when the group has offsets on only a subset of the requested partitions", func(t *testing.T) {
		// Regression test: a group that has only partially committed offsets (e.g. a topic
		// partition count increase, or a crash mid-alter) must not be treated as fully
		// initialized, or the missing partitions would never get an install/inherit pass.
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 42}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		search := MustParseGroupId("test")
		result := indexer.exists(search, []TopicPartition{
			{Partition: 0, Topic: "test-topic"},
			{Partition: 1, Topic: "test-topic"}, // no committed offset for this partition
		})
		assertions.False(result)
	})

	t.Run("should return false when the group's committed offsets are for a different topic", func(t *testing.T) {
		// Regression test: offsetsIndexerImpl.topic must actually be used to filter
		// ListConsumerGroupOffsets results. A group id shared across topics (or one that
		// only ever consumed a different topic) must not be treated as existing for this
		// indexer's topic merely because Kafka has some committed offsets under that name.
		nativeAdminAdapter := NewMockNativeAdminAdapter(gomock.NewController(t))
		nativeAdminAdapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
			{GroupId: "test"},
		}, nil)
		nativeAdminAdapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "test").Return(
			map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "other-topic"}: {Offset: 42}}, nil)

		indexer, err := newOffsetIndexer(ctx, "test", "test-topic", nativeAdminAdapter)
		assertions.NoError(err)

		search := MustParseGroupId("test")
		result := indexer.exists(search, []TopicPartition{{Partition: 0, Topic: "test-topic"}})
		assertions.False(result)
	})
}

func TestOffsetIndexer_findPreviousStateOffset_excludesCurrentGroup(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()

	// Regression test: a plain group must never be offered its own committed offsets as
	// an "ancestor" merely because the current GroupId is a distinct, freshly-parsed
	// pointer with the same name as the one already indexed.
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
