package debefix

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"
)

// FileProvider provides files and tags to Load. The order matters, so it should be deterministic.
type FileProvider interface {
	Load(FileProviderCallback) error
}

type FileProviderCallback func(info FileInfo) error

type FileInfo struct {
	Name string
	File io.Reader
	Tags []string
}

type directoryFileProvider struct {
	fs      fs.FS
	include func(path string, entry os.DirEntry) bool
	tagFunc func(dirs []string) []string
}

// NewDirectoryFileProvider creates a FileProvider that list files from a directory, sorted by name.
// Only files with the ".dbf.yaml" extension are returned.
// Returned file names are relative to the rootDir.
func NewDirectoryFileProvider(rootDir string, options ...DirectoryFileProviderOption) FileProvider {
	return NewDirectoryFileProviderFS(os.DirFS(rootDir), options...)
}

// NewDirectoryFileProviderFS creates a FileProvider that list files from a fs.FS, sorted by name.
// Only files with the ".dbf.yaml" extension are returned.
func NewDirectoryFileProviderFS(fs fs.FS, options ...DirectoryFileProviderOption) FileProvider {
	ret := &directoryFileProvider{
		fs: fs,
	}
	for _, opt := range options {
		opt(ret)
	}
	if ret.include == nil {
		ret.include = func(string, os.DirEntry) bool {
			return true
		}
	}
	if ret.tagFunc == nil {
		ret.tagFunc = noDirectoryTagFunc
	}
	return ret
}

type DirectoryFileProviderOption func(*directoryFileProvider)

// WithDirectoryIncludeFunc sets a callback to allow choosing files that will be read.
func WithDirectoryIncludeFunc(include func(path string, entry os.DirEntry) bool) DirectoryFileProviderOption {
	return func(provider *directoryFileProvider) {
		provider.include = include
	}
}

// WithDirectoryAsTag creates tags for each directory. Inner directories will be concatenated by a dot (.).
func WithDirectoryAsTag() DirectoryFileProviderOption {
	return func(provider *directoryFileProvider) {
		provider.tagFunc = DefaultDirectoryTagFunc
	}
}

// WithDirectoryTagFunc allows returning custom tags for each directory entry.
func WithDirectoryTagFunc(tagFunc func(dirs []string) []string) DirectoryFileProviderOption {
	return func(provider *directoryFileProvider) {
		provider.tagFunc = tagFunc
	}
}

// DefaultDirectoryTagFunc joins directories using a dot (.).
func DefaultDirectoryTagFunc(dirs []string) []string {
	return []string{strings.Join(dirs, ".")}
}

// noDirectoryTagFunc don't add tags to directories.
func noDirectoryTagFunc(dirs []string) []string {
	return nil
}

func (d directoryFileProvider) Load(f FileProviderCallback) error {
	return d.loadFiles(".", nil, f)
}

func (d directoryFileProvider) loadFiles(currentPath string, tags []string, f FileProviderCallback) error {
	files, err := d.readDirSorted(currentPath)
	if err != nil {
		return fmt.Errorf("error reading directory '%s': %w", currentPath, err)
	}

	var dirs []string

	for _, file := range files {
		if !d.include(currentPath, file) {
			continue
		}

		fullPath := path.Join(currentPath, file.Name())

		if file.IsDir() {
			dirs = append(dirs, file.Name())
			continue
		}

		if strings.HasSuffix(file.Name(), ".dbf.yaml") {
			localFile, err := d.fs.Open(fullPath)
			if err != nil {
				return fmt.Errorf("error opening file '%s': %w", fullPath, err)
			}

			err = f(FileInfo{
				Name: fullPath,
				File: localFile,
				Tags: d.tagFunc(tags),
			})

			fileErr := localFile.Close()
			if fileErr != nil {
				return errors.Join(fmt.Errorf("error closing file '%s': %w", fullPath, fileErr), err)
			}

			if err != nil {
				return fmt.Errorf("error processing file '%s': %w", fullPath, err)
			}
		}
	}

	for _, dir := range dirs {
		fullPath := path.Join(currentPath, dir)

		// each directory may become a tag
		err := d.loadFiles(fullPath, append(slices.Clone(tags), dir), f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d directoryFileProvider) readDirSorted(currentPath string) ([]os.DirEntry, error) {
	files, err := fs.ReadDir(d.fs, currentPath)
	if err != nil {
		return nil, err
	}

	slices.SortFunc(files, func(a, b os.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	return files, err
}
