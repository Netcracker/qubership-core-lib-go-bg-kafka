package blue_green_kafka

import (
	"context"
	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
)

func EventualStrategy() OffsetInheritanceStrategy {
	return func(ctx context.Context, current GroupId, previous []groupIdWithOffset) (map[TopicPartition]OffsetAndMetadata, error) {
		var previousStrs []string
		for _, g := range previous {
			previousStrs = append(previousStrs, g.String())
		}
		logger.InfoC(ctx, "Resolve offsets for versioned group: %s from previous stage offsets: %+v", current, previousStrs)
		var result map[TopicPartition]OffsetAndMetadata
		if _, ok := current.(*PlainGroupId); ok {
			if len(previous) == 0 {
				/* non BG case */
				result = nil
			} else {
				// Note: a plain group's own previously committed offsets are never present in
				// `previous` here - the indexer can only ever index one PlainGroupId per prefix
				// (its own, since PlainGroupId.GetGroupIdPrefix() == Name), and
				// findPreviousStateOffset excludes the current group from its own ancestor
				// list. Preserving an existing group's offsets is align()'s job (its exists()
				// check short-circuits before this strategy ever runs), not this strategy's.
				/* transition from old BG implementation */
				// inherit from BG1 ACTIVE group
				for _, gwo := range previous {
					if bg1vg, ok := gwo.groupId.(*BG1VersionedGroupId); ok && bg1vg.Stage == bg1StageActive {
						result = gwo.offset
						break
					}
				}
				if len(result) == 0 {
					/* destroy BG domain case */
					// inherit from ACTIVE group
					for _, gwo := range previous {
						if vg, ok := gwo.groupId.(*VersionedGroupId); ok && vg.State == bgMonitor.StateActive {
							result = gwo.offset
							break
						}
					}
				}
			}
		} else if currentVersioned, ok := current.(*VersionedGroupId); ok {
			if compareGroupStates(currentVersioned, bgMonitor.StateActive, bgMonitor.StateIdle) {
				var plainOffset map[TopicPartition]OffsetAndMetadata
				for _, gwo := range previous {
					if _, ok := gwo.groupId.(*PlainGroupId); ok {
						plainOffset = gwo.offset
						break
					}
				}
				if len(plainOffset) > 0 {
					/* InitDomain case */
					result = plainOffset
				} else {
					/* Commit case */
					// inherit from ACTIVE group
					for _, gwo := range previous {
						if vg, ok := gwo.groupId.(*VersionedGroupId); ok && vg.State == bgMonitor.StateActive {
							result = gwo.offset
							break
						}
					}
				}
			} else if compareGroupStates(currentVersioned, bgMonitor.StateActive, bgMonitor.StateCandidate) {
				var aiOffset map[TopicPartition]OffsetAndMetadata
				for _, gwo := range previous {
					if compareGroupStates(gwo.groupId, bgMonitor.StateActive, bgMonitor.StateIdle) {
						aiOffset = gwo.offset
						break
					}
				}
				if len(aiOffset) > 0 {
					/* Warmup case */
					result = aiOffset
				} else {
					/* Rollback case */
					for _, gwo := range previous {
						if compareGroupStates(gwo.groupId, bgMonitor.StateActive, bgMonitor.StateLegacy) {
							result = gwo.offset
							break
						}
					}
				}
			} else if compareGroupStates(currentVersioned, bgMonitor.StateActive, bgMonitor.StateLegacy) ||
				compareGroupStates(currentVersioned, bgMonitor.StateLegacy, bgMonitor.StateActive) {
				/* Promote case */
				// inherit from ACTIVE group
				for _, gwo := range previous {
					if vg, ok := gwo.groupId.(*VersionedGroupId); ok && vg.State == bgMonitor.StateActive {
						result = gwo.offset
						break
					}
				}
			} else if compareGroupStates(currentVersioned, bgMonitor.StateCandidate, bgMonitor.StateActive) {
				/* Warmup case */
				/* Rollback case */
				// inherit from ACTIVE group
				for _, gwo := range previous {
					if vg, ok := gwo.groupId.(*VersionedGroupId); ok && vg.State == bgMonitor.StateActive {
						result = gwo.offset
						break
					}
				}
			} else {
				logger.WarnC(ctx, "Unregistered states pair: '%s'+'%s' of consumer group: %s",
					currentVersioned.State.String(), currentVersioned.SiblingState.String(), currentVersioned.String())
			}
		} else {
			logger.WarnC(ctx, "Unknown groupId type: %s", current)
		}
		logger.InfoC(ctx, "Resolved offsets: %+v", result)
		return result, nil
	}
}
