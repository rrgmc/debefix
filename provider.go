package debefix_poc2

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

type FileProvider interface {
	Load(func(info FileInfo) error) error
}

type FileInfo struct {
	File io.Reader
	Tags []string
}

type directoryFileProvider struct {
	rootDir string
}

func NewDirectoryFileProvider(rootDir string) FileProvider {
	return &directoryFileProvider{rootDir: rootDir}
}

func (d directoryFileProvider) Load(f func(info FileInfo) error) error {
	return d.loadFiles(d.rootDir, nil, f)
}

func (d directoryFileProvider) loadFiles(path string, tags []string, f func(info FileInfo) error) error {
	files, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("error reading directory '%s': %w", path, err)
	}

	for _, file := range files {
		fullPath := filepath.Join(path, file.Name())

		if file.IsDir() {
			// each directory becomes a tag
			err := d.loadFiles(fullPath, append(slices.Clone(tags), file.Name()), f)
			if err != nil {
				return err
			}
		} else if strings.HasSuffix(file.Name(), ".dbf.yaml") {
			localFile, err := os.Open(fullPath)
			if err != nil {
				return fmt.Errorf("error opening file '%s': %w", fullPath, err)
			}

			err = f(FileInfo{
				File: localFile,
				Tags: tags,
			})

			fileErr := localFile.Close()
			if fileErr != nil {
				return fmt.Errorf("error closing file '%s': %w", fullPath, fileErr)
			}

			if err != nil {
				return fmt.Errorf("error processing file '%s': %w", fullPath, err)
			}
		}
	}

	return nil
}
