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

func TestMustParseGroupId(t *testing.T) {
	t.Run("should return group id when parsing succeeds", func(t *testing.T) {
		groupId := MustParseGroupId("plain-group")
		assert.IsType(t, &PlainGroupId{}, groupId)
	})

}

func TestToOffsetDateTime(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantErr   bool
		checkTime func(t *testing.T, result *time.Time)
	}{
		{
			name:    "valid datetime",
			input:   "2023-07-07_10-30-00",
			wantErr: false,
			checkTime: func(t *testing.T, result *time.Time) {
				expected := time.Date(2023, 7, 7, 10, 30, 0, 0, time.UTC)
				assert.Equal(t, expected, *result)
			},
		},
		{
			name:    "invalid format",
			input:   "invalid-datetime",
			wantErr: true,
		},
		{
			name:    "invalid year",
			input:   "invalid-07-07_10-30-00",
			wantErr: true,
		},
		{
			name:    "invalid month",
			input:   "2023-invalid-07_10-30-00",
			wantErr: true,
		},
		{
			name:    "invalid day",
			input:   "2023-07-invalid_10-30-00",
			wantErr: true,
		},
		{
			name:    "invalid hour",
			input:   "2023-07-07_invalid-30-00",
			wantErr: true,
		},
		{
			name:    "invalid minute",
			input:   "2023-07-07_10-invalid-00",
			wantErr: true,
		},
		{
			name:    "invalid second",
			input:   "2023-07-07_10-30-invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ToOffsetDateTime(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.checkTime != nil {
					tt.checkTime(t, result)
				}
			}
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name      string
		args      map[string]string
		wantErr   bool
		checkTime func(t *testing.T, result *time.Time)
	}{
		{
			name: "valid date",
			args: map[string]string{
				"year":   "2023",
				"month":  "7",
				"day":    "7",
				"hour":   "10",
				"minute": "30",
				"second": "0",
			},
			wantErr: false,
			checkTime: func(t *testing.T, result *time.Time) {
				expected := time.Date(2023, 7, 7, 10, 30, 0, 0, time.UTC)
				assert.Equal(t, expected, *result)
			},
		},
		{
			name: "invalid year",
			args: map[string]string{
				"year":   "invalid",
				"month":  "7",
				"day":    "7",
				"hour":   "10",
				"minute": "30",
				"second": "0",
			},
			wantErr: true,
		},
		{
			name: "invalid month",
			args: map[string]string{
				"year":   "2023",
				"month":  "invalid",
				"day":    "7",
				"hour":   "10",
				"minute": "30",
				"second": "0",
			},
			wantErr: true,
		},
		{
			name: "invalid day",
			args: map[string]string{
				"year":   "2023",
				"month":  "7",
				"day":    "invalid",
				"hour":   "10",
				"minute": "30",
				"second": "0",
			},
			wantErr: true,
		},
		{
			name: "invalid hour",
			args: map[string]string{
				"year":   "2023",
				"month":  "7",
				"day":    "7",
				"hour":   "invalid",
				"minute": "30",
				"second": "0",
			},
			wantErr: true,
		},
		{
			name: "invalid minute",
			args: map[string]string{
				"year":   "2023",
				"month":  "7",
				"day":    "7",
				"hour":   "10",
				"minute": "invalid",
				"second": "0",
			},
			wantErr: true,
		},
		{
			name: "invalid second",
			args: map[string]string{
				"year":   "2023",
				"month":  "7",
				"day":    "7",
				"hour":   "10",
				"minute": "30",
				"second": "invalid",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseDate(tt.args)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				if tt.checkTime != nil {
					tt.checkTime(t, result)
				}
			}
		})
	}
}

func TestBG1VersionedGroupId_GetGroupIdPrefix(t *testing.T) {
	groupId := &BG1VersionedGroupId{
		GroupIdPrefix: "test-prefix",
	}
	result := groupId.GetGroupIdPrefix()
	assert.Equal(t, "test-prefix", result)
}

func TestBG1VersionedGroupId_String(t *testing.T) {
	version := bgMonitor.NewVersionMust("v1")
	bgVersion := bgMonitor.NewVersionMust("v2")
	groupId := &BG1VersionedGroupId{
		GroupIdPrefix:    "test",
		Version:          *version,
		BlueGreenVersion: *bgVersion,
		Stage:            "a",
		Updated:          time.Unix(1234567890, 0),
	}
	result := groupId.String()
	expected := fmt.Sprintf("test-v1v2a1234567890")
	assert.Equal(t, expected, result)
}
