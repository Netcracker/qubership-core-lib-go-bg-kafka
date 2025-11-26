package blue_green_kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_align_groupExists_noBg1(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	indexer := NewMockOffsetsIndexer(controller)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    StrategyLatest,
		candidateOffsetSetupStrategy: StrategyLatest,
	}
	plainGroup := MustParseGroupId("test-plain-group")

	indexer.EXPECT().exists(plainGroup).Return(true)
	indexer.EXPECT().bg1VersionsExist().Return(false)
	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

func Test_align_groupExists_Bg1Migrated(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	indexer := NewMockOffsetsIndexer(controller)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    StrategyLatest,
		candidateOffsetSetupStrategy: StrategyLatest,
	}
	plainGroup := MustParseGroupId("test-plain-group")

	indexer.EXPECT().exists(plainGroup).Return(true)
	indexer.EXPECT().bg1VersionsExist().Return(true)
	indexer.EXPECT().bg1VersionsMigrated().Return(true)

	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

func Test_align_groupExists_Bg1NotMigrated(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	indexer := NewMockOffsetsIndexer(controller)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    StrategyLatest,
		candidateOffsetSetupStrategy: StrategyLatest,
	}
	plainGroup := MustParseGroupId("test-plain-group")

	indexer.EXPECT().exists(plainGroup).Return(true)
	indexer.EXPECT().findPreviousStateOffset(gomock.Any(), plainGroup).Return([]groupIdWithOffset{})
	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	adapter.EXPECT().EndOffsets(gomock.Any(), []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}, nil)
	adapter.EXPECT().AlterConsumerGroupOffsets(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	indexer.EXPECT().bg1VersionsExist().Return(true).AnyTimes()
	indexer.EXPECT().bg1VersionsMigrated().Return(false).AnyTimes()
	indexer.EXPECT().createMigrationDoneFromBg1MarkerGroup(gomock.Any()).Return(nil)

	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

func Test_align_groupNotExists(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	indexer := NewMockOffsetsIndexer(controller)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    StrategyLatest,
		candidateOffsetSetupStrategy: StrategyLatest,
	}
	plainGroup := MustParseGroupId("test-plain-group")

	indexer.EXPECT().exists(plainGroup).Return(false)
	indexer.EXPECT().findPreviousStateOffset(gomock.Any(), plainGroup).Return([]groupIdWithOffset{})
	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	adapter.EXPECT().EndOffsets(gomock.Any(), []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}, nil)
	adapter.EXPECT().AlterConsumerGroupOffsets(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	indexer.EXPECT().bg1VersionsExist().Return(false).AnyTimes()

	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

func TestOffsetCorrector_Install_StrategyEarliest(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	expectedOffsets := map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().BeginningOffsets(ctx, []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(expectedOffsets, nil)

	result, err := corrector.install(ctx, StrategyEarliest)
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 100}}, result)
}

func TestOffsetCorrector_Install_StrategyLatest(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	expectedOffsets := map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 200}

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().EndOffsets(ctx, []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(expectedOffsets, nil)

	result, err := corrector.install(ctx, StrategyLatest)
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 200}}, result)
}

func TestOffsetCorrector_Install_StrategyRewind(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: {Offset: 150, Timestamp: 1234567890}}
	expectedEndOffsets := map[TopicPartition]int64{topicPartition: 200}

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	adapter.EXPECT().EndOffsets(ctx, []TopicPartition{topicPartition}).Return(expectedEndOffsets, nil)

	result, err := corrector.install(ctx, Rewind(5*time.Minute))
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 150}}, result)
}

func TestOffsetCorrector_Install_StrategyRewindWithNilOffset(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: nil} // nil offset
	expectedEndOffsets := map[TopicPartition]int64{topicPartition: 200}

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	adapter.EXPECT().EndOffsets(ctx, []TopicPartition{topicPartition}).Return(expectedEndOffsets, nil)

	result, err := corrector.install(ctx, Rewind(5*time.Minute))
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 200}}, result)
}

func TestOffsetCorrector_Install_PartitionsForError(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	expectedErr := errors.New("partitions error")

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(nil, expectedErr)

	result, err := corrector.install(ctx, StrategyEarliest)
	assertions.Error(err)
	assertions.ErrorIs(err, expectedErr)
	assertions.Nil(result)
}

func TestOffsetCorrector_Install_BeginningOffsetsError(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	expectedErr := errors.New("beginning offsets error")

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().BeginningOffsets(ctx, []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(nil, expectedErr)

	result, err := corrector.install(ctx, StrategyEarliest)
	assertions.Error(err)
	assertions.Contains(err.Error(), "failed to calculate BeginningOffsets")
	assertions.Nil(result)
}

func TestOffsetCorrector_Install_EndOffsetsError(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	expectedErr := errors.New("end offsets error")

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().EndOffsets(ctx, []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(nil, expectedErr)

	result, err := corrector.install(ctx, StrategyLatest)
	assertions.Error(err)
	assertions.Contains(err.Error(), "failed to calculate EndOffsets")
	assertions.Nil(result)
}

func TestOffsetCorrector_Install_OffsetsForTimesError(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	expectedErr := errors.New("offsets for times error")

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(nil, expectedErr)

	result, err := corrector.install(ctx, Rewind(5*time.Minute))
	assertions.Error(err)
	assertions.Contains(err.Error(), "failed to calculate OffsetsForTimes")
	assertions.Nil(result)
}

func TestOffsetCorrector_Install_OffsetsForTimesEndOffsetsError(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	partitions := []PartitionInfo{{Partition: 0, Topic: "test-topic"}}
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: {Offset: 150, Timestamp: 1234567890}}
	expectedErr := errors.New("end offsets error")

	adapter.EXPECT().PartitionsFor(ctx, "test-topic").Return(partitions, nil)
	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	adapter.EXPECT().EndOffsets(ctx, []TopicPartition{topicPartition}).Return(nil, expectedErr)

	result, err := corrector.install(ctx, Rewind(5*time.Minute))
	assertions.Error(err)
	assertions.Contains(err.Error(), "failed to calculate EndOffsets")
	assertions.Nil(result)
}
