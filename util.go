package debefix

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-yaml/ast"
)

// appendStringNoRepeat appends strings to an array without repetitions.
func appendStringNoRepeat(src []string, tags []string) []string {
	for _, tag := range tags {
		if !slices.Contains(src, tag) {
			src = append(src, tag)
		}
	}
	return src
}

// getStringNode gets the string value of a string node, or an error if not a string node.
func getStringNode(node ast.Node) (string, error) {
	switch n := node.(type) {
	case *ast.StringNode:
		return n.Value, nil
	default:
		return "", NewParseError("node is not string", node.GetPath(), node.GetToken().Position)
	}
}

// sliceMap applies a function to each slice item and return the resulting slice.
func sliceMap[T any, U []T](ts U, f func(T) T) U {
	us := make(U, len(ts))
	for i := range ts {
		us[i] = f(ts[i])
	}
	return us
}

// stripNumberPunctuationPrefix removes [numberPunctuation] from the string prefix.
func stripNumberPunctuationPrefix(s string) string {
	isPrefix := true
	return strings.Map(func(r rune) rune {
		if !isPrefix || strings.IndexRune(numberPunctuation, r) < 0 {
			isPrefix = false
			return r
		}
		return -1
	}, s)
}

var numberPunctuation = "0123456789!\"#$%&'()*+,-./:;?@[\\]^_`{|}~"

func castToInt(v any) (any, error) {
	switch vv := v.(type) {
	case nil, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return v, nil
	case float32:
		return int64(vv), nil
	case float64:
		return int64(vv), nil
	case string:
		return strconv.ParseInt(vv, 10, 64)
	default:
		return strconv.ParseInt(fmt.Sprint(vv), 10, 64)
	}
}

func castToFloat(v any) (any, error) {
	switch vv := v.(type) {
	case nil, float32, float64:
		return v, nil
	case int:
		return float64(vv), nil
	case int8:
		return float64(vv), nil
	case int16:
		return float64(vv), nil
	case int32:
		return float64(vv), nil
	case int64:
		return float64(vv), nil
	case uint:
		return float64(vv), nil
	case uint8:
		return float64(vv), nil
	case uint16:
		return float64(vv), nil
	case uint32:
		return float64(vv), nil
	case uint64:
		return float64(vv), nil
	case string:
		return strconv.ParseFloat(vv, 64)
	default:
		return strconv.ParseFloat(fmt.Sprint(vv), 64)
	}
}

// This is a subset of the formats allowed by the regular expression
// defined at http://yaml.org/type/timestamp.html.
var allowedTimestampFormats = []string{
	"2006-1-2T15:4:5.999999999Z07:00", // RCF3339Nano with short date fields.
	"2006-1-2t15:4:5.999999999Z07:00", // RFC3339Nano with short date fields and lower-case "t".
	"2006-1-2 15:4:5.999999999",       // space separated with no time zone
	"2006-1-2",                        // date only
}

func castToTime(v any) (any, error) {
	switch v.(type) {
	case nil, time.Time:
		return v, nil
	default:
		var t time.Time
		var err error
		for _, format := range allowedTimestampFormats {
			t, err = time.Parse(format, fmt.Sprint(v))
			if err != nil {
				continue
			}
			return t, nil
		}
		return t, err
	}
}
