package postgres

import (
	"io/fs"

	"github.com/RangelReale/debefix"
	"github.com/RangelReale/debefix/db/sql"
)

func Generate(fileProvider debefix.FileProvider, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	return debefix.Generate(fileProvider, ResolverFunc(db), options...)
}

func GenerateFS(fs fs.FS, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	return debefix.GenerateFS(fs, ResolverFunc(db), options...)
}

func GenerateDirectory(rootDir string, db sql.QueryInterface, options ...debefix.GenerateOption) error {
	return debefix.GenerateDirectory(rootDir, ResolverFunc(db), options...)
}
