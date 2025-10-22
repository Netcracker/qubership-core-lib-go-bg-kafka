package blue_green_kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractHeaders(t *testing.T) {
	t.Run("should extract headers to map", func(t *testing.T) {
		headers := []Header{
			{Key: "x-version", Value: []byte("v1")},
			{Key: "x-trace-id", Value: []byte("trace-123")},
			{Key: "content-type", Value: []byte("application/json")},
		}

		result := ExtractHeaders(headers)

		expected := map[string]interface{}{
			"x-version":    "v1",
			"x-trace-id":   "trace-123",
			"content-type": "application/json",
		}
		assert.Equal(t, expected, result)
	})

	t.Run("should handle empty headers", func(t *testing.T) {
		headers := []Header{}

		result := ExtractHeaders(headers)

		expected := map[string]interface{}{}
		assert.Equal(t, expected, result)
	})

	t.Run("should handle nil headers", func(t *testing.T) {
		var headers []Header

		result := ExtractHeaders(headers)

		expected := map[string]interface{}{}
		assert.Equal(t, expected, result)
	})

	t.Run("should handle headers with empty values", func(t *testing.T) {
		headers := []Header{
			{Key: "empty-value", Value: []byte("")},
			{Key: "nil-value", Value: nil},
		}

		result := ExtractHeaders(headers)

		expected := map[string]interface{}{
			"empty-value": "",
			"nil-value":   "",
		}
		assert.Equal(t, expected, result)
	})
}

func TestBuildHeaders(t *testing.T) {
	t.Run("should build headers from context data", func(t *testing.T) {
		ctxData := map[string]string{
			"x-version":    "v1",
			"x-trace-id":   "trace-123",
			"content-type": "application/json",
		}

		result := BuildHeaders(ctxData)

		expected := []Header{
			{Key: "x-version", Value: []byte("v1")},
			{Key: "x-trace-id", Value: []byte("trace-123")},
			{Key: "content-type", Value: []byte("application/json")},
		}
		assert.ElementsMatch(t, expected, result)
	})

	t.Run("should handle empty context data", func(t *testing.T) {
		ctxData := map[string]string{}

		result := BuildHeaders(ctxData)

		assert.Nil(t, result)
	})

	t.Run("should handle nil context data", func(t *testing.T) {
		var ctxData map[string]string

		result := BuildHeaders(ctxData)

		assert.Nil(t, result)
	})

	t.Run("should handle empty string values", func(t *testing.T) {
		ctxData := map[string]string{
			"empty-value":  "",
			"normal-value": "test",
		}

		result := BuildHeaders(ctxData)

		expected := []Header{
			{Key: "empty-value", Value: []byte("")},
			{Key: "normal-value", Value: []byte("test")},
		}
		assert.ElementsMatch(t, expected, result)
	})
}
