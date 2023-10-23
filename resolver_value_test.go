package debefix

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"
)

func TestDefaultParseResolvedValue(t *testing.T) {
	tests := []struct {
		name          string
		typ           string
		value         any
		expectedNotOk bool
		expected      any
		expectedError string
	}{
		{
			name:     "int",
			typ:      "int",
			value:    12,
			expected: 12,
		},
		{
			name:     "int32",
			typ:      "int",
			value:    int32(12),
			expected: int32(12),
		},
		{
			name:     "int str",
			typ:      "int",
			value:    "12",
			expected: int64(12),
		},
		{
			name:          "int str error",
			typ:           "int",
			value:         "12.3",
			expectedError: "ParseInt",
		},
		{
			name:     "float",
			typ:      "float",
			value:    15.3,
			expected: 15.3,
		},
		{
			name:     "float32",
			typ:      "float",
			value:    float32(15.3),
			expected: float32(15.3),
		},
		{
			name:     "float str",
			typ:      "float",
			value:    "15.3",
			expected: float64(15.3),
		},
		{
			name:          "float str error",
			typ:           "float",
			value:         "abc",
			expectedError: "ParseFloat",
		},
		{
			name:     "str",
			typ:      "str",
			value:    "abc",
			expected: "abc",
		},
		{
			name:     "str int",
			typ:      "str",
			value:    56,
			expected: "56",
		},
		{
			name:     "time",
			typ:      "timestamp",
			value:    time.Date(2001, time.January, 1, 15, 15, 15, 0, time.UTC),
			expected: time.Date(2001, time.January, 1, 15, 15, 15, 0, time.UTC),
		},
		{
			name:     "time str",
			typ:      "timestamp",
			value:    "2001-01-01T15:15:15.000Z",
			expected: time.Date(2001, time.January, 1, 15, 15, 15, 0, time.UTC),
		},
		{
			name:          "unknown type",
			typ:           "unknown",
			expectedNotOk: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok, v, err := DefaultParseResolvedValue(test.typ, test.value)
			assert.Equal(t, !test.expectedNotOk, ok)
			if test.expectedError == "" {
				assert.NilError(t, err)
				if !test.expectedNotOk {
					assert.Equal(t, test.expected, v)
				}
			} else {
				assert.ErrorContains(t, err, test.expectedError)
			}
		})
	}
}
