package blue_green_kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffsetManager_alignOffset(t *testing.T) {
	t.Run("should align offset with eventual consistency mode", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockAdmin := NewMockNativeAdminAdapter(controller)

		config := BgKafkaConsumerConfig{
			ConsistencyMode:              func() ConsumerConsistencyMode { return Eventual },
			AdminSupplier:                func() (NativeAdminAdapter, error) { return mockAdmin, nil },
			Topic:                        func() string { return "test-topic" },
			ActiveOffsetSetupStrategy:    func() OffsetSetupStrategy { return StrategyLatest },
			CandidateOffsetSetupStrategy: func() OffsetSetupStrategy { return StrategyLatest },
		}

		manager := &offsetManager{config: config}
		groupId := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")

		// Mock the indexer creation and align call
		mockAdmin.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{}, nil)
		mockAdmin.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
		mockAdmin.EXPECT().EndOffsets(gomock.Any(), []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}, nil)
		mockAdmin.EXPECT().AlterConsumerGroupOffsets(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		err := manager.alignOffset(context.Background(), groupId)
		require.NoError(t, err)
	})

	t.Run("should align offset with guarantee consumption mode", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		mockAdmin := NewMockNativeAdminAdapter(controller)

		config := BgKafkaConsumerConfig{
			ConsistencyMode:              func() ConsumerConsistencyMode { return GuaranteeConsumption },
			AdminSupplier:                func() (NativeAdminAdapter, error) { return mockAdmin, nil },
			Topic:                        func() string { return "test-topic" },
			ActiveOffsetSetupStrategy:    func() OffsetSetupStrategy { return StrategyLatest },
			CandidateOffsetSetupStrategy: func() OffsetSetupStrategy { return StrategyLatest },
		}

		manager := &offsetManager{config: config}
		groupId := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")

		// Mock the indexer creation and align call
		mockAdmin.EXPECT().ListConsumerGroups(gomock.Any()).Return([]ConsumerGroup{}, nil)
		mockAdmin.EXPECT().PartitionsFor(gomock.Any(), "test-topic").Return([]PartitionInfo{{Partition: 0, Topic: "test-topic"}}, nil)
		mockAdmin.EXPECT().EndOffsets(gomock.Any(), []TopicPartition{{Partition: 0, Topic: "test-topic"}}).Return(map[TopicPartition]int64{{Partition: 0, Topic: "test-topic"}: 100}, nil)
		mockAdmin.EXPECT().AlterConsumerGroupOffsets(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		err := manager.alignOffset(context.Background(), groupId)
		require.NoError(t, err)
	})

	t.Run("should return error for unsupported consistency mode", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		config := BgKafkaConsumerConfig{
			ConsistencyMode: func() ConsumerConsistencyMode { return ConsumerConsistencyMode("INVALID") },
		}

		manager := &offsetManager{config: config}
		groupId := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")

		err := manager.alignOffset(context.Background(), groupId)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported consuming consistency mode")
	})

	t.Run("should return error when admin supplier fails", func(t *testing.T) {
		controller := gomock.NewController(t)
		defer controller.Finish()

		config := BgKafkaConsumerConfig{
			ConsistencyMode: func() ConsumerConsistencyMode { return Eventual },
			AdminSupplier:   func() (NativeAdminAdapter, error) { return nil, errors.New("admin error") },
		}

		manager := &offsetManager{config: config}
		groupId := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")

		err := manager.alignOffset(context.Background(), groupId)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get admin")
	})
}
