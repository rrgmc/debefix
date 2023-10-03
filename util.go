package debefix_poc2

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
)

func getStringNode(node ast.Node) (string, error) {
	switch n := node.(type) {
	case *ast.StringNode:
		return n.Value, nil
	default:
		return "", fmt.Errorf("node at '%s' is not string", node.GetPath())
	}
}
