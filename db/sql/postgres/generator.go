package postgres

import (
	"io/fs"

	"github.com/RangelReale/debefix"
	"github.com/RangelReale/debefix/db/sql"
)

// Generate loads files and inserts records in the database.
func Generate(fileProvider debefix.FileProvider, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	return debefix.Generate(fileProvider, ResolverFunc(db), options...)
}

// GenerateFS is a version of Generate that loads from a fs.FS.
func GenerateFS(fs fs.FS, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	return debefix.GenerateFS(fs, ResolverFunc(db), options...)
}

// GenerateDirectory is a version of Generate that loads from a directory name.
func GenerateDirectory(rootDir string, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	return debefix.GenerateDirectory(rootDir, ResolverFunc(db), options...)
}
