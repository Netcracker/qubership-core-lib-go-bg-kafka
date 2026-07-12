package blue_green_kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
)

type offsetCorrector struct {
	topic                        string
	indexer                      offsetsIndexer
	inherit                      OffsetInheritanceStrategy
	admin                        NativeAdminAdapter
	activeOffsetSetupStrategy    OffsetSetupStrategy
	candidateOffsetSetupStrategy OffsetSetupStrategy
}

func (oc *offsetCorrector) align(ctx context.Context, current GroupId) error {
	topicPartitions, err := oc.topicPartitions(ctx)
	if err != nil {
		return fmt.Errorf("failed to get partitions: %w", err)
	}
	// A group that already has committed offsets for every partition of this topic must never
	// have those offsets altered here, regardless of BG1 migration state - only the (separate)
	// migration-marker bookkeeping below still needs to run in that case.
	if oc.indexer.exists(current, topicPartitions) {
		logger.InfoC(ctx, "Skip group id offsets corrections for: '%s'", current.String())
		if oc.indexer.bg1VersionsExist() && !oc.indexer.bg1VersionsMigrated() {
			return oc.indexer.createMigrationDoneFromBg1MarkerGroup(ctx)
		}
		return nil
	}
	prevIds := oc.indexer.findPreviousStateOffset(ctx, current)
	var previousStrs []string
	for _, g := range prevIds {
		previousStrs = append(previousStrs, g.String())
	}
	logger.InfoC(ctx, "Inherit offset from previous: %+v", previousStrs)
	proposedOffset, err := oc.inherit(ctx, current, prevIds)
	if err != nil {
		return fmt.Errorf("failed to inherit offsets: %w", err)
	}
	if len(proposedOffset) == 0 {
		var strategy OffsetSetupStrategy
		if _, ok := current.(*PlainGroupId); ok {
			strategy = oc.activeOffsetSetupStrategy
		} else if vg, ok := current.(*VersionedGroupId); ok {
			switch vg.State {
			case bgMonitor.StateActive:
				strategy = oc.activeOffsetSetupStrategy
			case bgMonitor.StateCandidate:
				strategy = oc.candidateOffsetSetupStrategy
			default:
				strategy = Rewind(5 * time.Minute)
				logger.WarnC(ctx, "No proposed offset resolved for state '%v'. Using default: '%v'", vg.State, strategy)
			}
		}
		proposedOffset, err = oc.install(ctx, strategy, topicPartitions)
		if err != nil {
			return fmt.Errorf("failed to install offsets: %w", err)
		}
	}
	logger.InfoC(ctx, "Alter group %s offset to %+v", current, proposedOffset)
	err = oc.admin.AlterConsumerGroupOffsets(ctx, current, proposedOffset)
	if err != nil {
		return fmt.Errorf("failed to alter consumer group: %w", err)
	}
	if oc.indexer.bg1VersionsExist() && !oc.indexer.bg1VersionsMigrated() {
		return oc.indexer.createMigrationDoneFromBg1MarkerGroup(ctx)
	}
	return nil
}

// topicPartitions fetches the current set of partitions for this corrector's topic, used both
// to decide whether a group already has complete offsets (see exists()) and, if not, as the
// partition set to install offsets for.
func (oc *offsetCorrector) topicPartitions(ctx context.Context) ([]TopicPartition, error) {
	partitions, err := oc.admin.PartitionsFor(ctx, oc.topic)
	if err != nil {
		return nil, err
	}
	logger.InfoC(ctx, "topic: '%s' partitions=%+v", oc.topic, partitions)
	topicPartitions := make([]TopicPartition, 0, len(partitions))
	for _, pi := range partitions {
		topicPartitions = append(topicPartitions, TopicPartition{
			Partition: pi.Partition,
			Topic:     pi.Topic,
		})
	}
	return topicPartitions, nil
}

func (oc *offsetCorrector) install(ctx context.Context, strategy OffsetSetupStrategy, topicPartitions []TopicPartition) (map[TopicPartition]OffsetAndMetadata, error) {
	logger.InfoC(ctx, "Installing by strategy: %+v", strategy)
	var offsets map[TopicPartition]int64
	var err error
	if strategy == StrategyEarliest {
		logger.InfoC(ctx, "Calculating BeginningOffsets")
		offsets, err = oc.admin.BeginningOffsets(ctx, topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate BeginningOffsets: %w", err)
		}
		logger.InfoC(ctx, "Calculated BeginningOffsets=%+v", offsets)
		for _, tp := range topicPartitions {
			if _, ok := offsets[tp]; !ok {
				return nil, fmt.Errorf("no BeginningOffsets result for partition %+v", tp)
			}
		}
	} else if strategy == StrategyLatest {
		logger.InfoC(ctx, "Calculating EndOffsets")
		offsets, err = oc.admin.EndOffsets(ctx, topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate EndOffsets: %w", err)
		}
		logger.InfoC(ctx, "Calculated EndOffsets=%+v", offsets)
		for _, tp := range topicPartitions {
			if _, ok := offsets[tp]; !ok {
				return nil, fmt.Errorf("no EndOffsets result for partition %+v", tp)
			}
		}
	} else {
		query := map[TopicPartition]time.Time{}
		for _, e := range topicPartitions {
			query[e] = time.Now().Add(-strategy.shift)
		}
		logger.InfoC(ctx, "Calculating OffsetsForTimes, query=%+v", query)
		found, err := oc.admin.OffsetsForTimes(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate OffsetsForTimes: %w", err)
		}
		var foundOffsetsTimes []string
		for tp, of := range found {
			foundOffsetsTimes = append(foundOffsetsTimes, fmt.Sprintf("%v: %s", tp, of.String()))
		}
		logger.InfoC(ctx, "Calculated OffsetsForTimes=%+v", strings.Join(foundOffsetsTimes, ", "))
		logger.InfoC(ctx, "Calculating EndOffsets for topicPartitions=%+v", topicPartitions)
		fallback, err := oc.admin.EndOffsets(ctx, topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to calculate EndOffsets: %w", err)
		}
		logger.InfoC(ctx, "Calculated EndOffsets=%+v", fallback)
		offsets = map[TopicPartition]int64{}
		for _, topicPart := range topicPartitions {
			offsetTime, ok := found[topicPart]
			if !ok || offsetTime == nil {
				fallbackOffset, ok := fallback[topicPart]
				if !ok {
					return nil, fmt.Errorf("no EndOffsets fallback available for partition %+v", topicPart)
				}
				offsets[topicPart] = fallbackOffset
			} else {
				offsets[topicPart] = offsetTime.Offset
			}
		}
	}
	result := map[TopicPartition]OffsetAndMetadata{}
	for topicPart, offset := range offsets {
		result[topicPart] = OffsetAndMetadata{Offset: offset}
	}
	logger.InfoC(ctx, "Calculated Offsets=%+v", result)
	return result, nil
}
