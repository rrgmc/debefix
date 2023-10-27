package value

import (
	"github.com/goccy/go-yaml/ast"
	"github.com/rrgmc/debefix"
)

// getStringNode gets the string value of a string node, or an error if not a string node.
func getStringNode(node ast.Node) (string, error) {
	switch n := node.(type) {
	case *ast.StringNode:
		return n.Value, nil
	default:
		return "", debefix.NewParseError("node is not string", node.GetPath(), node.GetToken().Position)
	}
}
