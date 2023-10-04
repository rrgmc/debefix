package debefix

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDirectoryFileProviderChecksExtension(t *testing.T) {
	provider := NewFSFileProvider(testFS)

	files := map[string]bool{}

	err := provider.Load(func(info FileInfo) error {
		files[info.Name] = true
		return nil
	})
	require.NoError(t, err)

	// should not load file without .dbf.yaml
	require.Equal(t, len(testFS)-1, len(files), "loaded more files than expected")
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

		require.Equal(t, []string{tag}, info.Tags)
		return nil
	})
	require.NoError(t, err)
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

		require.Equal(t, []string{"a." + tag}, info.Tags)
		return nil
	})
	require.NoError(t, err)
}

func TestDirectoryFileProviderIgnoresFiles(t *testing.T) {
	provider := NewFSFileProvider(testFS, WithDirectoryIncludeFunc(func(path string, entry os.DirEntry) bool {
		return !strings.HasPrefix(path, "test1/inner")
	}))

	err := provider.Load(func(info FileInfo) error {
		require.False(t, strings.HasPrefix(info.Name, "test1/inner/"))
		return nil
	})
	require.NoError(t, err)
}
