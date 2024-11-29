package debefix

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound = errors.New("not found")
)

// ResolveError is the base of all returned errors.
type ResolveError struct {
	Err error
}

func NewResolveError(msg string) *ResolveError {
	return &ResolveError{Err: errors.New(msg)}
}

func NewResolveErrorf(format string, args ...any) *ResolveError {
	return &ResolveError{Err: fmt.Errorf(format, args...)}
}

func (e *ResolveError) Error() string {
	return e.Err.Error()
}

func (e *ResolveError) Unwrap() error {
	return e.Err
}
