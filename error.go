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
)

type ParseError struct {
	ErrorMessage string
	Path         string
	Token        *token.Token
}

func NewParseError(msg string, path string, token *token.Token) ParseError {
	return ParseError{
		ErrorMessage: msg,
		Path:         path,
		Token:        token,
	}
}

func (e ParseError) Error() string {
	return fmt.Sprintf("%s: %s", e.Path, e.ErrorMessage)
}
