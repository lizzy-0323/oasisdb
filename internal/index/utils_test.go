package index

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringToInt32(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected bool // true if we expect numeric conversion, false if we expect hash
	}{
		{
			name:     "numeric string",
			input:    "123",
			expected: true,
		},
		{
			name:     "zero string",
			input:    "0",
			expected: true,
		},
		{
			name:     "negative number",
			input:    "-456",
			expected: true,
		},
		{
			name:     "alphanumeric string",
			input:    "abc123",
			expected: false,
		},
		{
			name:     "special characters",
			input:    "doc-id#100",
			expected: false,
		},
		{
			name:     "empty string",
			input:    "",
			expected: true, // empty string converts to 0
		},
	}

	// Test that numeric strings convert as expected
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := stringToInt32(tc.input)
			
			if tc.expected {
				// For numeric strings, check direct conversion
				expected, _ := strconv.Atoi(tc.input)
				assert.Equal(t, int32(expected), result, "Numeric string should convert directly")
			} else {
				// For non-numeric strings, ensure we don't get 0
				assert.NotEqual(t, int32(0), result, "Non-numeric string should not convert to 0")
				
				// Test consistency - same input should always give same output
				result2 := stringToInt32(tc.input)
				assert.Equal(t, result, result2, "Hash result should be consistent")
				
				// Different non-numeric inputs should produce different hashes
				if tc.name != "empty string" {
					differentResult := stringToInt32(tc.input + "different")
					assert.NotEqual(t, result, differentResult, "Different inputs should produce different hashes")
				}
			}
		})
	}
}
