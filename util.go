package debefix

import (
	"slices"

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
		return "", NewParseError("node is not string", node.GetPath(), node.GetToken())
	}
}
