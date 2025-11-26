package blue_green_kafka

import (
	"context"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
)

func GuaranteeConsistencyStrategy() OffsetInheritanceStrategy {
	return func(ctx context.Context, current GroupId, previous []groupIdWithOffset) (map[TopicPartition]OffsetAndMetadata, error) {
		var previousStrings []string
		for _, g := range previous {
			previousStrings = append(previousStrings, g.String())
		}
		logger.InfoC(ctx, "Resolve offsets for versioned group: %s from previous stage offsets: %+v", current, previousStrings)
		result := map[TopicPartition]OffsetAndMetadata{}
		var err error
		// Promote case : (ACTIVE, CANDIDATE),
		// Rollback case : (ACTIVE, LEGACY)
		// Commit case : (ACTIVE, IDLE)
		prevHasActive := false
		for _, gwo := range previous {
			if bg1vg, ok := gwo.groupId.(*VersionedGroupId); ok && bg1vg.State == bgMonitor.StateActive {
				prevHasActive = true
				break
			}
		}
		if currentVersioned, ok := current.(*VersionedGroupId); ok &&
			(compareGroupStates(currentVersioned, bgMonitor.StateActive, bgMonitor.StateCandidate) ||
				compareGroupStates(currentVersioned, bgMonitor.StateActive, bgMonitor.StateLegacy) ||
				compareGroupStates(currentVersioned, bgMonitor.StateActive, bgMonitor.StateIdle)) && prevHasActive {
			topicPartitionsSet := map[TopicPartition]struct{}{}
			for _, gwo := range previous {
				for tp := range gwo.offset {
					topicPartitionsSet[tp] = struct{}{}
				}
			}
			for tp := range topicPartitionsSet {
				var minOffset *OffsetAndMetadata
				for _, gwo := range previous {
					offset := gwo.offset[tp]
					if minOffset == nil {
						minOffset = &offset
					} else if offset.Offset < minOffset.Offset {
						minOffset = &offset
					}
				}
				if minOffset != nil {
					result[tp] = *minOffset
				}
			}
		} else {
			result, err = EventualStrategy()(ctx, current, previous)
			if err != nil {
				return nil, err
			}
		}
		logger.InfoC(ctx, "Resolved offsets: %+v", result)
		return result, err
	}
}
