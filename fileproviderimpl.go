package debefix

import (
	"cmp"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
)

const (
	dirConfigFilename = "dbfconfig.yaml"
)

type DirConfig struct {
	Config DirConfigConfig `yaml:"config"`
}

type DirConfigConfig struct {
	Tags      []string `yaml:"tags"`       // tags to apply recursively
	LocalTags []string `yaml:"local_tags"` // tags to apply only to the current folder
}

type fsFileProvider struct {
	fs                fs.FS
	include           func(path string, entry os.DirEntry) bool
	tagFunc           func(dirs []string) []string
	skipDirConfigFile bool
}

// NewDirectoryFileProvider creates a [FileProvider] that list files from a directory, sorted by name.
// Only files with the ".dbf.yaml" extension are returned.
// Returned file names are relative to the rootDir.
// If a folder has a "dbfconfig.yaml" file, its settings will be applied to the entire folder.
func NewDirectoryFileProvider(rootDir string, options ...FSFileProviderOption) FileProvider {
	return NewFSFileProvider(os.DirFS(rootDir), options...)
}

// NewFSFileProvider creates a [FileProvider] that list files from a [fs.FS], sorted by name.
// Only files with the ".dbf.yaml" extension are returned.
// If a folder has a "dbfconfig.yaml" file, its settings will be applied to the entire folder.
func NewFSFileProvider(fs fs.FS, options ...FSFileProviderOption) FileProvider {
	ret := &fsFileProvider{
		fs: fs,
	}
	for _, opt := range options {
		opt.apply(ret)
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

// WithDirectoryIncludeFunc sets a callback to allow choosing files that will be read.
// Check entry [os.DirEntry.IsDir] to detect files or directories.
func WithDirectoryIncludeFunc(include func(path string, entry os.DirEntry) bool) FSFileProviderOption {
	return fnFSFileProviderOption(func(provider *fsFileProvider) {
		provider.include = include
	})
}

// WithDirectoryAsTag creates tags for each directory. Inner directories will be concatenated by a dot (.).
func WithDirectoryAsTag() FSFileProviderOption {
	return fnFSFileProviderOption(func(provider *fsFileProvider) {
		provider.tagFunc = DefaultDirectoryTagFunc
	})
}

// WithDirectoryTagFunc allows returning custom tags for each directory entry.
func WithDirectoryTagFunc(tagFunc func(dirs []string) []string) FSFileProviderOption {
	return fnFSFileProviderOption(func(provider *fsFileProvider) {
		provider.tagFunc = tagFunc
	})
}

// WithSkipDirConfigFile skips loading a "dbfconfig.yaml" per folder.
func WithSkipDirConfigFile() FSFileProviderOption {
	return fnFSFileProviderOption(func(provider *fsFileProvider) {
		provider.skipDirConfigFile = true
	})
}

// DefaultDirectoryTagFunc joins directories using a dot (.).
func DefaultDirectoryTagFunc(dirs []string) []string {
	return []string{strings.Join(dirs, ".")}
}

// StripNumberPunctuationPrefixDirectoryTagFunc strips number and punctuation prefixes from each
// dir (like "01-") and joins directories using a dot (.).
func StripNumberPunctuationPrefixDirectoryTagFunc(dirs []string) []string {
	stripDirs := sliceMap[string](dirs, func(s string) string {
		return stripNumberPunctuationPrefix(s)
	})
	return []string{strings.Join(stripDirs, ".")}
}

// noDirectoryTagFunc don't add tags to directories.
func noDirectoryTagFunc(dirs []string) []string {
	return nil
}

func (d fsFileProvider) Load(f FileProviderCallback) error {
	return d.loadFiles(".", nil, nil, f)
}

func (d fsFileProvider) loadFiles(currentPath string, dirs []string, tags []string, f FileProviderCallback) error {
	files, err := d.readDirSorted(currentPath)
	if err != nil {
		return fmt.Errorf("error reading directory '%s': %w", currentPath, err)
	}

	var childDirst []string
	var currentDirTags []string

	// load dir config file if available
	if !d.skipDirConfigFile {
		for _, file := range files {
			if file.IsDir() {
				continue
			}

			if file.Name() == dirConfigFilename {
				fullPath := path.Join(currentPath, file.Name())

				var dc DirConfig
				dcFile, err := d.fs.Open(fullPath)
				if err != nil {
					return fmt.Errorf("error opening dir config file '%s': %w", fullPath, err)
				}

				err = yaml.NewDecoder(dcFile, yaml.Strict()).Decode(&dc)

				fileErr := dcFile.Close()
				if fileErr != nil {
					return errors.Join(fmt.Errorf("error closing dir config file '%s': %w", fullPath, fileErr), err)
				}

				if err != nil {
					return fmt.Errorf("error processing file '%s': %w", fullPath, err)
				}

				currentDirTags = dc.Config.LocalTags
				tags = slices.Concat(tags, dc.Config.Tags)
				break
			}
		}
	}

	for _, file := range files {
		if !d.include(currentPath, file) {
			continue
		}

		fullPath := path.Join(currentPath, file.Name())

		if file.IsDir() {
			childDirst = append(childDirst, file.Name())
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
				Tags: slices.Concat(tags, currentDirTags, d.tagFunc(dirs)),
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

	for _, dir := range childDirst {
		fullPath := path.Join(currentPath, dir)

		// each directory may become a tag
		err := d.loadFiles(fullPath, append(slices.Clone(dirs), dir), tags, f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d fsFileProvider) readDirSorted(currentPath string) ([]os.DirEntry, error) {
	files, err := fs.ReadDir(d.fs, currentPath)
	if err != nil {
		return nil, err
	}

	slices.SortFunc(files, func(a, b os.DirEntry) int {
		return cmp.Compare(a.Name(), b.Name())
	})

	return files, err
}

// NewStringFileProvider creates a [FileProvider] that simulates a file for each string field, in the array order.
func NewStringFileProvider(files []string, options ...StringFileProviderOption) FileProvider {
	ret := &stringFileProvider{files: files}
	for _, opt := range options {
		opt(ret)
	}
	return ret
}

// WithStringFileProviderTags sets tags using the same array indexes as the files parameter.
func WithStringFileProviderTags(tags [][]string) StringFileProviderOption {
	return func(p *stringFileProvider) {
		p.tags = tags
	}
}

type stringFileProvider struct {
	files []string
	tags  [][]string
}

func (s stringFileProvider) Load(callback FileProviderCallback) error {
	digitSize := fmt.Sprintf("%d", len(s.files))
	fileFmt := fmt.Sprintf("%%0%dd-file.dbf.yaml", len(digitSize)+1)

	for idx, data := range s.files {
		var tags []string
		if idx < len(s.tags) {
			tags = s.tags[idx]
		}
		err := callback(FileInfo{
			Name: fmt.Sprintf(fileFmt, idx),
			File: strings.NewReader(data),
			Tags: tags,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
