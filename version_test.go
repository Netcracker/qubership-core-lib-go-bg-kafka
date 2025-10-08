package blue_green_kafka

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseGroupId_ErrorCases(t *testing.T) {
	assertions := require.New(t)

	t.Run("should return PlainGroupId for invalid format", func(t *testing.T) {
		groupId, err := ParseGroupId("invalid-group-id")
		assertions.NoError(err)
		plainId := groupId.(*PlainGroupId)
		assertions.Equal("invalid-group-id", plainId.Name)
	})

	t.Run("should return PlainGroupId for empty string", func(t *testing.T) {
		groupId, err := ParseGroupId("")
		assertions.NoError(err)
		plainId := groupId.(*PlainGroupId)
		assertions.Equal("", plainId.Name)
	})

	t.Run("should return PlainGroupId for invalid timestamp format", func(t *testing.T) {
		groupId, err := ParseGroupId("test-v1-a_i-invalid-timestamp")
		assertions.NoError(err)
		plainId := groupId.(*PlainGroupId)
		assertions.Equal("test-v1-a_i-invalid-timestamp", plainId.Name)
	})
}

func TestMustParseGroupId_Success(t *testing.T) {
	assertions := require.New(t)

	t.Run("should return PlainGroupId for invalid format", func(t *testing.T) {
		groupId := MustParseGroupId("invalid-group-id")
		plainId := groupId.(*PlainGroupId)
		assertions.Equal("invalid-group-id", plainId.Name)
	})

	t.Run("should return PlainGroupId for empty string", func(t *testing.T) {
		groupId := MustParseGroupId("")
		plainId := groupId.(*PlainGroupId)
		assertions.Equal("", plainId.Name)
	})
}

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
