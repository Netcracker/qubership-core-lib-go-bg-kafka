package blue_green_kafka

import (
	"fmt"
	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"regexp"
	"strconv"
	"time"
)

// GroupId implementations are pointers: compare with Equals (or String()), never ==.
type GroupId interface {
	GetGroupIdPrefix() string
	String() string
	Equals(other GroupId) bool
}

type VersionedGroupId struct {
	GroupIdPrefix string
	Version       bgMonitor.Version
	State         bgMonitor.State
	SiblingState  bgMonitor.State
	Updated       time.Time
}

type PlainGroupId struct {
	Name string
}

const (
	bg1StageActive   = "a"
	bg1StageMigrated = "M"
)

type BG1VersionedGroupId struct {
	GroupIdPrefix    string
	Version          bgMonitor.Version
	BlueGreenVersion bgMonitor.Version
	Stage            string
	Updated          time.Time
}

type groupIdWithOffset struct {
	groupId GroupId
	offset  map[TopicPartition]OffsetAndMetadata
}

func (g *groupIdWithOffset) String() string {
	return fmt.Sprintf("groupId: '%s', offset: %v", g.groupId.String(), g.offset)
}

var stateCharPattern = regexp.MustCompile(`[acli]`)
var dateTimePatternString = `(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})-(\d{2})`
var dateTimePattern = regexp.MustCompile(dateTimePatternString)
var datePartTemplate = "%02d-%02d-%02d_%02d-%02d-%02d"
var versionedPattern = *regexp.MustCompile(`(.+)-(v\d+)-(` + stateCharPattern.String() + `)_(` + stateCharPattern.String() + `)-(` + dateTimePattern.String() + `)`)
var bg1VersionedPattern = *regexp.MustCompile(`(.+)-(v\d+)(v\d+)([lacrM])(\d+)`)

func ParseGroupId(name string) (GroupId, error) {
	if versionedPattern.MatchString(name) {
		matched := versionedPattern.FindStringSubmatch(name)
		errWrap := func(err error) error { return fmt.Errorf("failed to parse group name: %w", err) }
		prefix := matched[1]
		version, err := bgMonitor.NewVersion(matched[2])
		if err != nil {
			return nil, errWrap(err)
		}
		v, err := bgMonitor.StateFromShort(matched[3])
		if err != nil {
			return nil, errWrap(err)
		}
		sv, err := bgMonitor.StateFromShort(matched[4])
		if err != nil {
			return nil, errWrap(err)
		}
		dateTime, err := ToOffsetDateTime(matched[5])
		if err != nil {
			return nil, errWrap(err)
		}
		return &VersionedGroupId{
			GroupIdPrefix: prefix,
			Version:       *version,
			State:         v,
			SiblingState:  sv,
			Updated:       *dateTime,
		}, nil
	} else if bg1VersionedPattern.MatchString(name) {
		matched := bg1VersionedPattern.FindStringSubmatch(name)
		errWrap := func(err error) error { return fmt.Errorf("failed to parse bg1 group name: %w", err) }
		prefix := matched[1]
		version, err := bgMonitor.NewVersion(matched[2])
		if err != nil {
			return nil, errWrap(err)
		}
		blueGreenVersion, err := bgMonitor.NewVersion(matched[3])
		if err != nil {
			return nil, errWrap(err)
		}
		stage := matched[4]
		unixTime, err := strconv.ParseInt(matched[5], 10, 0)
		if err != nil {
			return nil, errWrap(err)
		}
		dateTime := time.Unix(unixTime, 0)
		return &BG1VersionedGroupId{
			GroupIdPrefix:    prefix,
			Version:          *version,
			BlueGreenVersion: *blueGreenVersion,
			Stage:            stage,
			Updated:          dateTime,
		}, nil
	} else {
		return &PlainGroupId{Name: name}, nil
	}
}

func MustParseGroupId(name string) GroupId {
	groupId, err := ParseGroupId(name)
	if err != nil {
		panic(err)
	}
	return groupId
}

func (v *VersionedGroupId) GetGroupIdPrefix() string {
	return v.GroupIdPrefix
}

func (v *VersionedGroupId) String() string {
	return fmt.Sprintf("%s-%s-%s_%s-%s",
		v.GroupIdPrefix, v.Version.String(), v.State.ShortString(), v.SiblingState.ShortString(), FromOffsetDateTime(v.Updated))
}

func (v *VersionedGroupId) Equals(other GroupId) bool {
	return other != nil && v.String() == other.String()
}

func FromOffsetDateTime(dt time.Time) string {
	return fmt.Sprintf(datePartTemplate, dt.Year(), int(dt.Month()), dt.Day(), dt.Hour(), dt.Minute(), dt.Second())
}

func ToOffsetDateTime(dateTime string) (*time.Time, error) {
	if !dateTimePattern.MatchString(dateTime) {
		return nil, fmt.Errorf("failed to parse dateTime '%s'. Invalid format. Must match the following pattern: '%s'",
			dateTime, dateTimePattern.String())
	}
	matched := dateTimePattern.FindStringSubmatch(dateTime)
	return parseDate(map[string]string{
		"year":   matched[1],
		"month":  matched[2],
		"day":    matched[3],
		"hour":   matched[4],
		"minute": matched[5],
		"second": matched[6],
	})
}

func parseDate(args map[string]string) (*time.Time, error) {
	dateArgs := make(map[string]int, len(args))
	for fieldName, fieldValue := range args {
		n, err := strconv.Atoi(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", fieldName, err)
		}
		dateArgs[fieldName] = n
	}
	dt := time.Date(dateArgs["year"], time.Month(dateArgs["month"]), dateArgs["day"], dateArgs["hour"], dateArgs["minute"], dateArgs["second"], 0, time.UTC)
	return &dt, nil
}

func (v *BG1VersionedGroupId) GetGroupIdPrefix() string {
	return v.GroupIdPrefix
}

func (v *BG1VersionedGroupId) String() string {
	return fmt.Sprintf("%s-%s%s%s%d",
		v.GroupIdPrefix, v.Version.String(), v.BlueGreenVersion.String(), v.Stage, v.Updated.Unix())
}

func (v *BG1VersionedGroupId) Equals(other GroupId) bool {
	return other != nil && v.String() == other.String()
}

func (v *PlainGroupId) GetGroupIdPrefix() string {
	return v.Name
}

func (v *PlainGroupId) String() string {
	return v.Name
}

func (v *PlainGroupId) Equals(other GroupId) bool {
	return other != nil && v.String() == other.String()
}
