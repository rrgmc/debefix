package debefix

import (
	"testing"

	"gotest.tools/v3/assert"
)

func TestStripNumberPunctuationPrefix(t *testing.T) {
	tests := []struct {
		name     string
		str      string
		expected string
	}{
		{
			name:     "numbers",
			str:      "01test",
			expected: "test",
		},
		{
			name:     "punctuation",
			str:      ":test",
			expected: "test",
		},
		{
			name:     "numbers and punctuation",
			str:      "01-test",
			expected: "test",
		},
		{
			name:     "numbers and punctuation mix",
			str:      ":1x1test",
			expected: "x1test",
		},
		{
			name:     "numbers after alpha",
			str:      "01-test5",
			expected: "test5",
		},
		{
			name:     "japanese chars",
			str:      "01-JP-日本",
			expected: "JP-日本",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ret := stripNumberPunctuationPrefix(test.str)
			assert.Equal(t, test.expected, ret)
		})
	}
}
