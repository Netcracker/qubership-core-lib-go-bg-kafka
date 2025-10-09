package blue_green_kafka

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"testing"
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
