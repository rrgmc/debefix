package debefix_poc2

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// FileProvider provides files and tags to Load. The order matters, so it should be deterministic.
type FileProvider interface {
	Load(FileProviderCallback) error
}

type FileProviderCallback func(info FileInfo) error

type FileInfo struct {
	File io.Reader
	Tags []string
}

type directoryFileProvider struct {
	rootDir string
	include func(path string, entry os.DirEntry) bool
	tagFunc func(dirs []string) []string
}

// NewDirectoryFileProvider creates a FileProvider that list files from a directory, sorted by name.
// Only files with the ".dbf.yaml" extension are returned.
func NewDirectoryFileProvider(rootDir string, options ...DirectoryFileProviderOption) FileProvider {
	ret := &directoryFileProvider{
		rootDir: rootDir,
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

// WithDirectoryFileProviderIncludeFunc sets a callback to allow choosing files that will be read.
func WithDirectoryFileProviderIncludeFunc(include func(path string, entry os.DirEntry) bool) DirectoryFileProviderOption {
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
	return d.loadFiles(d.rootDir, nil, f)
}

func (d directoryFileProvider) loadFiles(path string, tags []string, f func(info FileInfo) error) error {
	files, err := d.readDirSorted(path)
	if err != nil {
		return fmt.Errorf("error reading directory '%s': %w", path, err)
	}

	var dirs []string

	for _, file := range files {
		if !d.include(path, file) {
			continue
		}

		fullPath := filepath.Join(path, file.Name())

		if file.IsDir() {
			dirs = append(dirs, file.Name())
			continue
		}

		if strings.HasSuffix(file.Name(), ".dbf.yaml") {
			localFile, err := os.Open(fullPath)
			if err != nil {
				return fmt.Errorf("error opening file '%s': %w", fullPath, err)
			}

			err = f(FileInfo{
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
		fullPath := filepath.Join(path, dir)

		// each directory may become a tag
		err := d.loadFiles(fullPath, append(slices.Clone(tags), dir), f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d directoryFileProvider) readDirSorted(path string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("error reading directory '%s': %w", path, err)
	}

	slices.SortFunc(files, func(a, b os.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	return files, err
}
