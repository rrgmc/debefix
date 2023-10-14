package debefix

import (
	"errors"
	"fmt"

	"github.com/goccy/go-yaml/token"
)

var (
	ValueError           = errors.New("value error")
	ResolveValueError    = errors.New("resolve value error")
	ResolveError         = errors.New("resolve error")
	ResolveCallbackError = errors.New("resolve callback error")
	RowNotFound          = errors.New("row not found in data")
)

type TokenPosition = token.Position

type ParseError struct {
	ErrorMessage string
	Path         string
	Position     *TokenPosition
}

func NewParseError(msg string, path string, position *TokenPosition) ParseError {
	return ParseError{
		ErrorMessage: msg,
		Path:         path,
		Position:     position,
	}
}

func (e ParseError) Error() string {
	return fmt.Sprintf("%s: %s", e.Path, e.ErrorMessage)
}
