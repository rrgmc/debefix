package debefix

import (
	"io"
)

// FileProvider provides files and tags to [Load]. The order matters, so it should be deterministic.
type FileProvider interface {
	Load(FileProviderCallback) error
}

type FileProviderCallback func(info FileInfo) error

type FileInfo struct {
	Name string
	File io.Reader
	Tags []string
}
