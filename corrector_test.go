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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	indexer.EXPECT().exists(plainGroup, topicPartitions).Return(true)
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	indexer.EXPECT().exists(plainGroup, topicPartitions).Return(true)
	indexer.EXPECT().bg1VersionsExist().Return(true)
	indexer.EXPECT().bg1VersionsMigrated().Return(true)

	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

// An existing group's offsets must stay untouched even with unmigrated BG1 groups present;
// only migration-marker bookkeeping should run for it.
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	indexer.EXPECT().exists(plainGroup, topicPartitions).Return(true)
	indexer.EXPECT().bg1VersionsExist().Return(true)
	indexer.EXPECT().bg1VersionsMigrated().Return(false)
	indexer.EXPECT().createMigrationDoneFromBg1MarkerGroup(gomock.Any()).Return(nil)

	// no findPreviousStateOffset/EndOffsets/AlterConsumerGroupOffsets expectations: offsets stay untouched
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	indexer.EXPECT().exists(plainGroup, topicPartitions).Return(false)
	indexer.EXPECT().findPreviousStateOffset(gomock.Any(), plainGroup).Return([]groupIdWithOffset{})
	adapter.EXPECT().EndOffsets(gomock.Any(), topicPartitions).Return(map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}, nil)
	adapter.EXPECT().AlterConsumerGroupOffsets(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)
	indexer.EXPECT().bg1VersionsExist().Return(false).AnyTimes()

	err := corrector.align(context.Background(), plainGroup)
	assertions.NoError(err)
}

// A plain group with committed offsets on multiple partitions must survive a consumer
// restart untouched (exists()/findPreviousStateOffset() used to compare GroupId by pointer
// identity, so a freshly re-parsed GroupId was never recognized as already existing).
func Test_align_existingPlainGroupWithMultiplePartitions_survivesRestart(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)

	groupName := "test-plain-group-multi-partition"
	committedOffsets := map[TopicPartition]OffsetAndMetadata{
		{Partition: 0, Topic: "test-topic"}: {Offset: 100},
		{Partition: 1, Topic: "test-topic"}: {Offset: 200},
	}
	adapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
		{GroupId: groupName},
	}, nil)
	adapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), groupName).Return(committedOffsets, nil)

	indexer, err := newOffsetIndexer(ctx, groupName, "test-topic", adapter)
	assertions.NoError(err)

	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    Rewind(5 * time.Minute),
		candidateOffsetSetupStrategy: Rewind(5 * time.Minute),
	}

	// current is a distinct GroupId instance from the one indexed above, as on every restart
	current, err := ParseGroupId(groupName)
	assertions.NoError(err)

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{
		{Partition: 0, Topic: "test-topic"}, {Partition: 1, Topic: "test-topic"},
	}, nil)
	// no OffsetsForTimes/EndOffsets/AlterConsumerGroupOffsets expectations: skip correction entirely
	err = corrector.align(ctx, current)
	assertions.NoError(err)
}

// The skip must be unconditional on existence: a leftover BG1 group in a stage other than
// active/migrated must not bypass it and force a rewind of an existing group's offsets.
func Test_align_existingPlainGroupWithUnmigratedBg1Leftover_survivesRestart(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)

	groupName := "orders"
	committedOffsets := map[TopicPartition]OffsetAndMetadata{
		{Partition: 0, Topic: "test-topic"}: {Offset: 500},
	}
	// leftover BG1 group, stage neither active nor migrated
	bg1LeftoverOffsets := map[TopicPartition]OffsetAndMetadata{
		{Partition: 0, Topic: "test-topic"}: {Offset: 10},
	}
	adapter.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{
		{GroupId: groupName},
		{GroupId: "orders-v1v1l1700000000"},
	}, nil)
	adapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), groupName).Return(committedOffsets, nil)
	adapter.EXPECT().ListConsumerGroupOffsets(gomock.Any(), "orders-v1v1l1700000000").Return(bg1LeftoverOffsets, nil)

	indexer, err := newOffsetIndexer(ctx, groupName, "test-topic", adapter)
	assertions.NoError(err)

	corrector := &offsetCorrector{
		topic:                        "test-topic",
		indexer:                      indexer,
		inherit:                      EventualStrategy(),
		admin:                        adapter,
		activeOffsetSetupStrategy:    Rewind(5 * time.Minute),
		candidateOffsetSetupStrategy: Rewind(5 * time.Minute),
	}
	current, err := ParseGroupId(groupName)
	assertions.NoError(err)

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
	// no AlterConsumerGroupOffsets expectation: no active-stage BG1 group, so
	// createMigrationDoneFromBg1MarkerGroup warns and returns nil without altering anything
	err = corrector.align(ctx, current)
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}
	expectedOffsets := map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}

	adapter.EXPECT().BeginningOffsets(ctx, topicPartitions).Return(expectedOffsets, nil)

	result, err := corrector.install(ctx, StrategyEarliest, topicPartitions)
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 100}}, result)
}

func TestOffsetCorrector_Install_StrategyEarliest_MissingPartition(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}, {Partition: 1, Topic: "test-topic"}}
	// BeginningOffsets omits partition 1 entirely; must error, not silently drop it
	adapter.EXPECT().BeginningOffsets(ctx, topicPartitions).Return(map[TopicPartition]int64{
		{Partition: 0, Topic: "test-topic"}: 100,
	}, nil)

	result, err := corrector.install(ctx, StrategyEarliest, topicPartitions)
	assertions.Error(err)
	assertions.Contains(err.Error(), "no BeginningOffsets result")
	assertions.Nil(result)
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}
	expectedOffsets := map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 200}

	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(expectedOffsets, nil)

	result, err := corrector.install(ctx, StrategyLatest, topicPartitions)
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{{Partition: 0, Topic: "test-topic"}: {Offset: 200}}, result)
}

func TestOffsetCorrector_Install_StrategyLatest_MissingPartition(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}, {Partition: 1, Topic: "test-topic"}}
	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(map[TopicPartition]int64{
		{Partition: 0, Topic: "test-topic"}: 200,
	}, nil)

	result, err := corrector.install(ctx, StrategyLatest, topicPartitions)
	assertions.Error(err)
	assertions.Contains(err.Error(), "no EndOffsets result")
	assertions.Nil(result)
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
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	topicPartitions := []TopicPartition{topicPartition}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: {Offset: 150, Timestamp: 1234567890}}
	expectedEndOffsets := map[TopicPartition]int64{topicPartition: 200}

	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(expectedEndOffsets, nil)

	result, err := corrector.install(ctx, Rewind(5*time.Minute), topicPartitions)
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
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	topicPartitions := []TopicPartition{topicPartition}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: nil} // nil offset
	expectedEndOffsets := map[TopicPartition]int64{topicPartition: 200}

	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(expectedEndOffsets, nil)

	result, err := corrector.install(ctx, Rewind(5*time.Minute), topicPartitions)
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 200}}, result)
}

func TestOffsetCorrector_Install_StrategyRewind_PartitionMissingFromOffsetsForTimes(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	topicPartitions := []TopicPartition{topicPartition}
	// partition absent from the response entirely, not just nil; must still fall back
	expectedEndOffsets := map[TopicPartition]int64{topicPartition: 200}

	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(map[TopicPartition]*OffsetAndTimestamp{}, nil)
	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(expectedEndOffsets, nil)

	result, err := corrector.install(ctx, Rewind(5*time.Minute), topicPartitions)
	assertions.NoError(err)
	assertions.Equal(map[TopicPartition]OffsetAndMetadata{topicPartition: {Offset: 200}}, result)
}

func TestOffsetCorrector_topicPartitions_Error(t *testing.T) {
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

	result, err := corrector.topicPartitions(ctx)
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}
	expectedErr := errors.New("beginning offsets error")

	adapter.EXPECT().BeginningOffsets(ctx, topicPartitions).Return(nil, expectedErr)

	result, err := corrector.install(ctx, StrategyEarliest, topicPartitions)
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}
	expectedErr := errors.New("end offsets error")

	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(nil, expectedErr)

	result, err := corrector.install(ctx, StrategyLatest, topicPartitions)
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
	topicPartitions := []TopicPartition{{Partition: 0, Topic: "test-topic"}}
	expectedErr := errors.New("offsets for times error")

	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(nil, expectedErr)

	result, err := corrector.install(ctx, Rewind(5*time.Minute), topicPartitions)
	assertions.Error(err)
	assertions.Contains(err.Error(), "failed to calculate OffsetsForTimes")
	assertions.Nil(result)
}

func TestOffsetCorrector_Install_StrategyRewind_MissingFallback(t *testing.T) {
	assertions := require.New(t)
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)
	corrector := &offsetCorrector{
		topic: "test-topic",
		admin: adapter,
	}

	ctx := context.Background()
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	topicPartitions := []TopicPartition{topicPartition}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: nil} // no record at/after the timestamp

	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	// EndOffsets fallback also omits the partition; must error, not commit offset 0
	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(map[TopicPartition]int64{}, nil)

	result, err := corrector.install(ctx, Rewind(5*time.Minute), topicPartitions)
	assertions.Error(err)
	assertions.Contains(err.Error(), "no EndOffsets fallback available")
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
	topicPartition := TopicPartition{Partition: 0, Topic: "test-topic"}
	topicPartitions := []TopicPartition{topicPartition}
	expectedTimes := map[TopicPartition]*OffsetAndTimestamp{topicPartition: {Offset: 150, Timestamp: 1234567890}}
	expectedErr := errors.New("end offsets error")

	adapter.EXPECT().OffsetsForTimes(gomock.Any(), gomock.Any()).Return(expectedTimes, nil)
	adapter.EXPECT().EndOffsets(ctx, topicPartitions).Return(nil, expectedErr)

	result, err := corrector.install(ctx, Rewind(5*time.Minute), topicPartitions)
	assertions.Error(err)
	assertions.Contains(err.Error(), "failed to calculate EndOffsets")
	assertions.Nil(result)
}
