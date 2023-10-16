package postgres

import (
	"io/fs"

	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db/sql"
)

// Generate loads files and inserts records in the database.
func Generate(fileProvider debefix.FileProvider, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	_, err := debefix.Generate(fileProvider, ResolverFunc(db), options...)
	return err
}

// GenerateFS is a version of Generate that loads from a fs.FS.
func GenerateFS(fs fs.FS, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	_, err := debefix.GenerateFS(fs, ResolverFunc(db), options...)
	return err
}

// GenerateDirectory is a version of Generate that loads from a directory name.
func GenerateDirectory(rootDir string, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	_, err := debefix.GenerateDirectory(rootDir, ResolverFunc(db), options...)
	return err
}
