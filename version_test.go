package blue_green_kafka

import (
	"fmt"
	"testing"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlainGroupId_GetGroupIdPrefix(t *testing.T) {
	assertions := require.New(t)

	groupId := MustParseGroupId("plain-group")
	pgId := groupId.(*PlainGroupId)

	result := pgId.GetGroupIdPrefix()
	assertions.Equal("plain-group", result)
}

func TestPlainGroupId_String(t *testing.T) {
	assertions := require.New(t)

	groupId := MustParseGroupId("plain-group")
	pgId := groupId.(*PlainGroupId)

	result := pgId.String()
	assertions.Equal("plain-group", result)
}

func TestVersionedGroupId_GetGroupIdPrefix(t *testing.T) {
	assertions := require.New(t)

	groupId := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")
	vgId := groupId.(*VersionedGroupId)

	result := vgId.GetGroupIdPrefix()
	assertions.Equal("test", result)
}

func TestVersionedGroupId_String(t *testing.T) {
	assertions := require.New(t)

	groupId := MustParseGroupId("test-v1-a_i-2023-07-07_10-30-00")
	vgId := groupId.(*VersionedGroupId)

	result := vgId.String()
	assertions.Equal("test-v1-a_i-2023-07-07_10-30-00", result)
}

func TestFromOffsetDateTime(t *testing.T) {
	assertions := require.New(t)

	t.Run("should create timestamp string from datetime", func(t *testing.T) {
		datetime := time.Date(2023, 7, 7, 10, 30, 0, 0, time.UTC)
		timestamp := FromOffsetDateTime(datetime)

		assertions.Equal("2023-07-07_10-30-00", timestamp)
	})
}

func TestParseGroupId(t *testing.T) {
	timeStr := "2025-01-02_03-04-05"
	time, err := time.Parse("2006-01-02_15-04-05", timeStr)
	assert.NoError(t, err)

	type args struct {
		name string
	}
	tests := []struct {
		name    string
		args    args
		want    GroupId
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "plain group",
			args:    struct{ name string }{name: "plain-group-name"},
			want:    &PlainGroupId{Name: "plain-group-name"},
			wantErr: assert.NoError,
		},
		{
			name: "versioned pattern group",
			args: struct{ name string }{name: "groupA-v1-a_c-" + timeStr},
			want: &VersionedGroupId{
				GroupIdPrefix: "groupA",
				Version:       *bgMonitor.NewVersionMust("v1"),
				State:         bgMonitor.StateActive,
				SiblingState:  bgMonitor.StateCandidate,
				Updated:       time,
			},
			wantErr: assert.NoError,
		},
		{
			name: "bg1 versioned pattern group",
			args: struct{ name string }{name: fmt.Sprintf("groupA-v5v8a%d", time.Unix())},
			want: &BG1VersionedGroupId{
				GroupIdPrefix:    "groupA",
				Version:          *bgMonitor.NewVersionMust("v5"),
				BlueGreenVersion: *bgMonitor.NewVersionMust("v8"),
				Stage:            "a",
				Updated:          time.Local(),
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseGroupId(tt.args.name)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseGroupId(%v)", tt.args.name)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ParseGroupId(%v)", tt.args.name)
		})
	}
}
