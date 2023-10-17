package debefix

import (
	"os"
	"strings"
	"testing"

	"gotest.tools/v3/assert"
)

func TestDirectoryFileProviderChecksExtension(t *testing.T) {
	provider := NewFSFileProvider(testFS)

	files := map[string]bool{}

	err := provider.Load(func(info FileInfo) error {
		files[info.Name] = true
		return nil
	})
	assert.NilError(t, err)

	// should not load file without .dbf.yaml
	assert.Equal(t, len(testFS)-1, len(files), "loaded more files than expected")
}

func TestDirectoryFileProviderDirectoryAsTags(t *testing.T) {
	provider := NewFSFileProvider(testFS, WithDirectoryAsTag())

	err := provider.Load(func(info FileInfo) error {
		var tag string
		if strings.HasPrefix(info.Name, "test1/inner/") {
			tag = "test1.inner"
		} else {
			tag, _, _ = strings.Cut(info.Name, "/")
		}

		assert.DeepEqual(t, []string{tag}, info.Tags)
		return nil
	})
	assert.NilError(t, err)
}

func TestDirectoryFileProviderDirectoryAsTagsFunc(t *testing.T) {
	provider := NewFSFileProvider(testFS, WithDirectoryTagFunc(func(dirs []string) []string {
		return []string{"a." + strings.Join(dirs, ".")}
	}))

	err := provider.Load(func(info FileInfo) error {
		var tag string
		if strings.HasPrefix(info.Name, "test1/inner/") {
			tag = "test1.inner"
		} else {
			tag, _, _ = strings.Cut(info.Name, "/")
		}

		assert.DeepEqual(t, []string{"a." + tag}, info.Tags)
		return nil
	})
	assert.NilError(t, err)
}

func TestDirectoryFileProviderIgnoresFiles(t *testing.T) {
	provider := NewFSFileProvider(testFS, WithDirectoryIncludeFunc(func(path string, entry os.DirEntry) bool {
		return !strings.HasPrefix(path, "test1/inner")
	}))

	err := provider.Load(func(info FileInfo) error {
		assert.Assert(t, !strings.HasPrefix(info.Name, "test1/inner/"))
		return nil
	})
	assert.NilError(t, err)
}
