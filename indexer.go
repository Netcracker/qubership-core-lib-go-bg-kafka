package blue_green_kafka

import (
	"context"
	"fmt"
	"strings"
	"time"
)

//go:generate mockgen -source=indexer.go -destination=indexer_mock.go -package=blue_green_kafka -mock_names=offsetsIndexer=MockOffsetsIndexer

type offsetsIndexer interface {
	// exists reports whether search already has committed offsets for every one of partitions.
	exists(search GroupId, partitions []TopicPartition) bool
	bg1VersionsExist() bool
	bg1VersionsMigrated() bool
	findPreviousStateOffset(ctx context.Context, current GroupId) []groupIdWithOffset
	createMigrationDoneFromBg1MarkerGroup(ctx context.Context) error
}

type offsetsIndexerImpl struct {
	topic string
	// keyed by GroupId.String() rather than the GroupId interface value itself: GroupId
	// implementations are pointers, so map keys of type GroupId compare by address, not
	// by content, and would never match a freshly-parsed GroupId with the same name.
	index map[string]groupIdWithOffset
	admin NativeAdminAdapter
}

type TopicPartition struct {
	Partition int
	Topic     string
}

type OffsetAndMetadata struct {
	Offset int64
}

type ConsumerGroup struct {
	GroupId string
}

func newOffsetIndexer(ctx context.Context, groupIdPrefix string, topic string, adminAdapter NativeAdminAdapter) (*offsetsIndexerImpl, error) {
	index := make(map[string]groupIdWithOffset)
	groups, err := adminAdapter.ListConsumerGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}
	for _, cg := range groups {
		if !strings.HasPrefix(cg.GroupId, groupIdPrefix) {
			// ignore unrelated groups
			continue
		}
		groupId, err := ParseGroupId(cg.GroupId)
		if err != nil {
			return nil, fmt.Errorf("failed to parse GroupId: %w", err)
		}
		if groupIdPrefix != groupId.GetGroupIdPrefix() {
			continue
		}
		offsets, err := adminAdapter.ListConsumerGroupOffsets(ctx, cg.GroupId)
		if err != nil {
			return nil, fmt.Errorf("failed to list consumer group offsets: %w", err)
		}
		// A consumer group can carry committed offsets for topics other than this indexer's
		// topic (e.g. the same group id used across topics). Only offsets for our own topic
		// are relevant to ancestor lookup and to "does this group already exist" checks.
		topicOffsets := make(map[TopicPartition]OffsetAndMetadata, len(offsets))
		for tp, o := range offsets {
			if tp.Topic == topic {
				topicOffsets[tp] = o
			}
		}
		if len(topicOffsets) == 0 {
			// no committed offsets for this topic; treat the group as not yet indexed so the
			// corrector still runs initial installation/inheritance for it
			continue
		}
		logger.InfoC(ctx, "Offsets=%+v", topicOffsets)
		logger.InfoC(ctx, "Adding to index groupId=%s with offset: %+v", groupId.String(), topicOffsets)
		index[groupId.String()] = groupIdWithOffset{groupId: groupId, offset: topicOffsets}
	}
	return &offsetsIndexerImpl{topic: topic, index: index, admin: adminAdapter}, nil
}

func (indexer *offsetsIndexerImpl) exists(search GroupId, partitions []TopicPartition) bool {
	e, ok := indexer.index[search.String()]
	if !ok {
		return false
	}
	for _, tp := range partitions {
		if _, ok := e.offset[tp]; !ok {
			return false
		}
	}
	return true
}

func (indexer *offsetsIndexerImpl) bg1VersionsExist() bool {
	for _, e := range indexer.index {
		if _, ok := e.groupId.(*BG1VersionedGroupId); ok {
			return true
		}
	}
	return false
}

func (indexer *offsetsIndexerImpl) bg1VersionsMigrated() bool {
	for _, e := range indexer.index {
		if bg1g, ok := e.groupId.(*BG1VersionedGroupId); ok && bg1g.Stage == bg1StageMigrated {
			return true
		}
	}
	return false
}

func (indexer *offsetsIndexerImpl) createMigrationDoneFromBg1MarkerGroup(ctx context.Context) error {
	var filtered []groupIdWithOffset
	for _, e := range indexer.index {
		if vg, ok := e.groupId.(*BG1VersionedGroupId); ok && vg.Stage == bg1StageActive {
			filtered = append(filtered, e)
		}
	}
	latestGroupOffset := getLatestGroupOffset(filtered)
	if latestGroupOffset != nil {
		bg1vg := latestGroupOffset.groupId.(*BG1VersionedGroupId)
		migratedMarkerGroup := &BG1VersionedGroupId{
			GroupIdPrefix:    bg1vg.GetGroupIdPrefix(),
			Version:          bg1vg.Version,
			BlueGreenVersion: bg1vg.BlueGreenVersion,
			Stage:            bg1StageMigrated,
			Updated:          time.Now(),
		}
		logger.InfoC(ctx, "Creating migrated from BG1 marker Group: %s", migratedMarkerGroup.String())
		return indexer.admin.AlterConsumerGroupOffsets(ctx, migratedMarkerGroup, latestGroupOffset.offset)
	} else {
		logger.WarnC(ctx, "Did not create 'migrated from BG1 marker Group' because no bg1 active group was found")
		return nil
	}
}

func (indexer *offsetsIndexerImpl) findPreviousStateOffset(ctx context.Context, current GroupId) []groupIdWithOffset {
	logger.InfoC(ctx, "Search the ancestor for: %s", current)
	currentKey := current.String()
	var filtered []groupIdWithOffset

	for key, e := range indexer.index {
		// filter out current group
		if key == currentKey {
			continue
		}
		// filter out offsets of the same update time (already created in sibling ns)
		if vg, ok1 := e.groupId.(*VersionedGroupId); ok1 {
			if vCurrent, ok2 := current.(*VersionedGroupId); ok2 &&
				(vg.Updated.Equal(vCurrent.Updated) || vg.Updated.After(vCurrent.Updated)) {
				continue
			}
		}
		filtered = append(filtered, e)
	}
	// find latest groupId
	latestGroupOffset := getLatestGroupOffset(filtered)
	var result []groupIdWithOffset
	if latestGroupOffset != nil {
		// if latest groupId is versioned - find all groupIds of the same updateTime
		if vg1, ok1 := latestGroupOffset.groupId.(*VersionedGroupId); ok1 {
			for _, e := range indexer.index {
				if vg2, ok2 := e.groupId.(*VersionedGroupId); ok2 && vg2.Updated.Equal(vg1.Updated) {
					result = append(result, e)
				}
			}
		} else if vg1, ok1 := latestGroupOffset.groupId.(*BG1VersionedGroupId); ok1 {
			// old blue-green groupId, we are interested only in active groupId
			for _, e := range indexer.index {
				if vg2, ok2 := e.groupId.(*BG1VersionedGroupId); ok2 && vg2.Updated.Equal(vg1.Updated) && vg2.Stage == bg1StageActive {
					result = append(result, e)
				}
			}
		} else {
			result = append(result, *latestGroupOffset)
		}
	}
	if len(result) > 0 {
		var previousStrs []string
		for _, g := range result {
			previousStrs = append(previousStrs, g.String())
		}
		logger.InfoC(ctx, "Ancestors for: '%s' are: %+v", current, previousStrs)
	} else {
		logger.InfoC(ctx, "There are no ancestors for: '%s'", current)
	}
	return result
}

func getLatestGroupOffset(entries []groupIdWithOffset) *groupIdWithOffset {
	var latest *groupIdWithOffset
	for i := range entries {
		if latest == nil || getSortKeyFromGroup(entries[i].groupId).After(getSortKeyFromGroup(latest.groupId)) {
			latest = &entries[i]
		}
	}
	return latest
}

var minTime = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

func getSortKeyFromGroup(g GroupId) time.Time {
	if vg, ok := g.(*VersionedGroupId); ok {
		return vg.Updated
	} else if vg, ok := g.(*BG1VersionedGroupId); ok {
		return vg.Updated
	} else {
		return minTime
	}
}
