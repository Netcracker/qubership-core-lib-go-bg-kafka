package blue_green_kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetrics_updateMetricsOnPoll(t *testing.T) {
	const (
		topic = "test-topic"

		partitionIdx1 = 1
		partitionIdx2 = 2
	)

	tests := []struct {
		name            string
		update          []*pollMetricsUpdate
		commitOffset    *PartitionedInt64Metric
		expectedMetrics *Metrics
	}{
		{
			name: "should update high watermark, last poll offset and poll counters",
			update: []*pollMetricsUpdate{
				{
					partition:     partitionIdx1,
					highWaterMark: 5,
					currentOffset: 1,
					isAccepted:    false,
				},
				{
					partition:     partitionIdx1,
					highWaterMark: 6,
					currentOffset: 3,
					isAccepted:    true,
				},
				{
					partition:     partitionIdx2,
					highWaterMark: 100,
					currentOffset: 99,
					isAccepted:    true,
				},
				{
					partition:     partitionIdx1,
					highWaterMark: 100,
					currentOffset: 19,
					isAccepted:    true,
				},
			},
			commitOffset: &PartitionedInt64Metric{
				values: map[int]int64{
					// commit 20 msgs for 1'st partition and 100 msgs for 2'nd partition
					partitionIdx1: 19,
					partitionIdx2: 99,
				},
			},
			expectedMetrics: &Metrics{
				Topic: topic,
				HighWatermark: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 100,
						partitionIdx2: 100,
					},
				},
				LastPollOffset: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 19,
						partitionIdx2: 99,
					},
				},
				Lag: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 80,
						partitionIdx2: 0,
					},
				},
				ConsumedMessageCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 3,
						partitionIdx2: 1,
					},
				},
				AcceptedMessageCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 2,
						partitionIdx2: 1,
					},
				},
				FilteredMessageCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 1,
					},
				},
			},
		},
		{
			name: "should update only counters for outdated offset data",
			update: []*pollMetricsUpdate{
				{
					partition:     partitionIdx1,
					highWaterMark: 5,
					currentOffset: 2,
					isAccepted:    true,
				},
				{
					partition:     partitionIdx1,
					highWaterMark: 4,
					currentOffset: 1,
					isAccepted:    false,
				},
			},
			expectedMetrics: &Metrics{
				Topic: topic,
				HighWatermark: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 5,
					},
				},
				LastPollOffset: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 2,
					},
				},
				Lag: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 5,
					},
				},
				ConsumedMessageCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 2,
					},
				},
				AcceptedMessageCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 1,
					},
				},
				FilteredMessageCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewConsumerMetrics(topic)
			if tt.commitOffset != nil {
				m.CommitOffset = tt.commitOffset
			}
			for _, update := range tt.update {
				m.updateMetricsOnPoll(update)
			}
			assert.Equal(t, tt.expectedMetrics.Topic, m.Topic, "unexpected topic name")
			assert.Equal(t, tt.expectedMetrics.HighWatermark, m.HighWatermark, "unexpected high water mark value")
			assert.Equal(t, tt.expectedMetrics.LastPollOffset, m.LastPollOffset, "unexpected current offset value")
			assert.Equal(t, tt.expectedMetrics.Lag, m.Lag, "unexpected lag value")
			assert.Equal(t, tt.expectedMetrics.ConsumedMessageCount, m.ConsumedMessageCount, "unexpected consumed message count")
			assert.Equal(t, tt.expectedMetrics.AcceptedMessageCount, m.AcceptedMessageCount, "unexpected accepted message count")
			assert.Equal(t, tt.expectedMetrics.FilteredMessageCount, m.FilteredMessageCount, "unexpected filtered message count")
		})
	}
}

func TestMetrics_updateMetricsOnCommit(t *testing.T) {
	const (
		topic = "test-topic"

		partitionIdx1 = 1
		partitionIdx2 = 2
	)

	tests := []struct {
		name            string
		update          *commitMetricsUpdate
		highWaterMark   *PartitionedInt64Metric
		expectedMetrics *Metrics
	}{
		{
			name: "should increase counter, update lag and commit offset for existing high watermark",
			update: &commitMetricsUpdate{
				partition:    partitionIdx1,
				commitOffset: 100,
			},
			highWaterMark: &PartitionedInt64Metric{
				values: map[int]int64{
					partitionIdx1: 111,
					partitionIdx2: 2,
				},
			},
			expectedMetrics: &Metrics{
				CommitOffset: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 100,
					},
				},
				Lag: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 10,
					},
				},
				CommitCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 1,
					},
				},
			},
		},
		{
			name: "should increase counter and update offset only (without lag) when no high watermark info",
			update: &commitMetricsUpdate{
				partition:    partitionIdx1,
				commitOffset: 100,
			},
			highWaterMark: &PartitionedInt64Metric{
				values: map[int]int64{
					partitionIdx2: 2,
				},
			},
			expectedMetrics: &Metrics{
				CommitOffset: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 100,
					},
				},
				Lag: newPartitionedInt64Metric(),
				CommitCount: &PartitionedInt64Metric{
					values: map[int]int64{
						partitionIdx1: 1,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewConsumerMetrics(topic)
			if tt.highWaterMark != nil {
				m.HighWatermark = tt.highWaterMark
			}
			m.updateMetricsOnCommit(tt.update)
			assert.Equal(t, tt.expectedMetrics.Lag, m.Lag, "unexpected lag value")
			assert.Equal(t, tt.expectedMetrics.CommitOffset, m.CommitOffset, "unexpected commit offset value")
			assert.Equal(t, tt.expectedMetrics.CommitCount, m.CommitCount, "unexpected commit count")
		})
	}
}

func TestMetrics_snapshot(t *testing.T) {
	t.Run("should create a copy of metrics", func(t *testing.T) {
		m := &Metrics{
			Topic: "topic",
			HighWatermark: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 5,
				},
			},
			LastPollOffset: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 2,
				},
			},
			CommitOffset: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 1,
				},
			},
			Lag: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 3,
				},
			},
			ConsumedMessageCount: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 2,
				},
			},
			AcceptedMessageCount: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 1,
				},
			},
			FilteredMessageCount: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 1,
				},
			},
			CommitCount: &PartitionedInt64Metric{
				values: map[int]int64{
					1: 1,
				},
			},
		}
		snapshot := m.snapshot()
		assert.Equal(t, snapshot.Topic, m.Topic, "unexpected topic name")
		assert.Equal(t, snapshot.HighWatermark, m.HighWatermark, "unexpected high water mark value")
		assert.Equal(t, snapshot.LastPollOffset, m.LastPollOffset, "unexpected current offset value")
		assert.Equal(t, snapshot.CommitOffset, m.CommitOffset, "unexpected commit offset value")
		assert.Equal(t, snapshot.Lag, m.Lag, "unexpected lag value")
		assert.Equal(t, snapshot.ConsumedMessageCount, m.ConsumedMessageCount, "unexpected consumed message count")
		assert.Equal(t, snapshot.AcceptedMessageCount, m.AcceptedMessageCount, "unexpected accepted message count")
		assert.Equal(t, snapshot.FilteredMessageCount, m.FilteredMessageCount, "unexpected filtered message count")
		assert.Equal(t, snapshot.CommitCount, m.CommitCount, "unexpected commit count")
	})
}

func TestMetrics_clean(t *testing.T) {
	t.Run("should clean read metrics data", func(t *testing.T) {
		testPartitionedMetric := &PartitionedInt64Metric{
			values: map[int]int64{
				1: 1,
				2: 0,
				3: 100,
			},
		}
		droppedPartitionMetric := &PartitionedInt64Metric{
			values: map[int]int64{},
		}
		topic := "test-topic"
		m := &Metrics{
			Topic:                topic,
			HighWatermark:        testPartitionedMetric,
			LastPollOffset:       testPartitionedMetric,
			CommitOffset:         testPartitionedMetric,
			Lag:                  testPartitionedMetric,
			ConsumedMessageCount: testPartitionedMetric,
			AcceptedMessageCount: testPartitionedMetric,
			FilteredMessageCount: testPartitionedMetric,
			CommitCount:          testPartitionedMetric,
		}

		m.clean()
		assert.Equal(t, topic, m.Topic, "unexpected topic name")
		assert.Equal(t, droppedPartitionMetric, m.HighWatermark, "unexpected high water mark value")
		assert.Equal(t, droppedPartitionMetric, m.LastPollOffset, "unexpected current offset value")
		assert.Equal(t, droppedPartitionMetric, m.CommitOffset, "unexpected commit offset value")
		assert.Equal(t, droppedPartitionMetric, m.Lag, "unexpected lag value")
		assert.Equal(t, droppedPartitionMetric, m.ConsumedMessageCount, "unexpected consumed message count")
		assert.Equal(t, droppedPartitionMetric, m.AcceptedMessageCount, "unexpected accepted message count")
		assert.Equal(t, droppedPartitionMetric, m.FilteredMessageCount, "unexpected filtered message count")
		assert.Equal(t, droppedPartitionMetric, m.CommitCount, "unexpected commit count")
	})
}

func TestPartitionedInt64Metric_GetByPartitions(t *testing.T) {
	t.Run("should return all values", func(t *testing.T) {
		metric := &PartitionedInt64Metric{
			values: map[int]int64{
				1: 100,
				2: 200,
				3: 300,
			},
		}

		result := metric.GetByPartitions()

		expected := map[int]int64{
			1: 100,
			2: 200,
			3: 300,
		}
		assert.Equal(t, expected, result)
	})

	t.Run("should return empty map for empty values", func(t *testing.T) {
		metric := &PartitionedInt64Metric{
			values: map[int]int64{},
		}

		result := metric.GetByPartitions()

		expected := map[int]int64{}
		assert.Equal(t, expected, result)
	})

	t.Run("should return single value", func(t *testing.T) {
		metric := &PartitionedInt64Metric{
			values: map[int]int64{
				1: 100,
			},
		}

		result := metric.GetByPartitions()

		expected := map[int]int64{
			1: 100,
		}
		assert.Equal(t, expected, result)
	})
}
