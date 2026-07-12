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

// Regression test: a group that already exists must have its offsets left untouched even
// when unmigrated BG1 groups are also present - only the (separate) migration-marker
// bookkeeping should still run. Before the fix, this combination of conditions caused the
// skip to be bypassed entirely, so an existing group's offsets were force-rewound via
// install()/Rewind(5m) on every restart (see corrector.go's exists() check).
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

	// No findPreviousStateOffset/EndOffsets/AlterConsumerGroupOffsets expectations are set:
	// the existing group's offsets must never be touched, only the migration marker created.
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

// Regression test for the production incident: a plain (non blue-green) group with
// committed offsets on multiple partitions must survive a consumer restart untouched.
// Before the fix, offsetsIndexerImpl.exists() and findPreviousStateOffset() compared
// GroupId values by pointer identity, so a freshly re-parsed GroupId with the same name
// as an already-committed group was never recognized as existing, fell through to the
// install path, and had its offsets force-rewound via Rewind(5m).
func Test_align_existingPlainGroupWithMultiplePartitions_survivesRestart(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)

	groupName := "history-events-a4c9373f"
	committedOffsets := map[TopicPartition]OffsetAndMetadata{
		{Partition: 0, Topic: "test-topic"}: {Offset: 6893},
		{Partition: 1, Topic: "test-topic"}: {Offset: 23081},
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

	// current is a distinct GroupId instance from the one indexed above, exactly as
	// happens on every real consumer restart (ParseGroupId / &PlainGroupId{} allocate fresh).
	current, err := ParseGroupId(groupName)
	assertions.NoError(err)

	adapter.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{
		{Partition: 0, Topic: "test-topic"}, {Partition: 1, Topic: "test-topic"},
	}, nil)
	// No OffsetsForTimes/EndOffsets/AlterConsumerGroupOffsets expectations are set: the
	// corrector must recognize the group already exists (with offsets for both partitions)
	// and skip correction entirely.
	err = corrector.align(ctx, current)
	assertions.NoError(err)
}

// Regression test for a residual force-rewind path: even after the group already exists, a
// leftover unmigrated BG1 group (any stage other than active/migrated) used to bypass the
// skip entirely and re-run install()/Rewind(5m) against the existing group's offsets. The
// skip must now be unconditional on existence; only migration-marker creation depends on
// BG1 state.
func Test_align_existingPlainGroupWithUnmigratedBg1Leftover_survivesRestart(t *testing.T) {
	assertions := require.New(t)
	ctx := context.Background()
	controller := gomock.NewController(t)
	adapter := NewMockNativeAdminAdapter(controller)

	groupName := "orders"
	committedOffsets := map[TopicPartition]OffsetAndMetadata{
		{Partition: 0, Topic: "test-topic"}: {Offset: 500},
	}
	// A leftover BG1 group with a non-active, non-migrated stage (e.g. "legacy"): present,
	// but neither an active source for migration nor already marked as migrated.
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
	// No AlterConsumerGroupOffsets expectation: bg1VersionsExist()=true but
	// bg1VersionsMigrated()=false (no active-stage BG1 group to migrate from), so
	// createMigrationDoneFromBg1MarkerGroup logs a warning and returns nil without altering
	// anything - the existing group's own offsets must remain untouched.
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
	// BeginningOffsets omits partition 1 entirely (e.g. adapter request-construction bug) -
	// the corrector must error rather than silently omitting that partition from the alter.
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
	// The partition is entirely absent from the OffsetsForTimes response (not just present
	// with a nil value) - the corrector must still fall back to EndOffsets for it.
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
	// EndOffsets omits the partition entirely (e.g. adapter request-construction bug) -
	// the corrector must error rather than silently committing offset 0.
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
