package debefix_poc2

import (
	"fmt"
	"slices"

	"github.com/goccy/go-yaml/ast"
)

func appendTags(src []string, tags []string) []string {
	for _, tag := range tags {
		if !slices.Contains(src, tag) {
			src = append(src, tag)
		}
	}
	return src
}

func getStringNode(node ast.Node) (string, error) {
	switch n := node.(type) {
	case *ast.StringNode:
		return n.Value, nil
	default:
		return "", fmt.Errorf("node at '%s' is not string", node.GetPath())
	}
}
