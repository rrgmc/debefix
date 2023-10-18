package debefix

import (
	"slices"
	"strings"

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
