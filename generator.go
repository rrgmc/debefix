package debefix

import "io/fs"

// Generate loads files and calls a resolver callback to resolve the values.
// It is a combination of [Load] and [Resolve].
func Generate(fileProvider FileProvider, resolver ResolveCallback, options ...GenerateOption) (*Data, error) {
	return generate(func(g *generator) FileProvider {
		return fileProvider
	}, resolver, options...)
}

// GenerateFS is a version of [Generate] that loads from a fs.FS.
func GenerateFS(fs fs.FS, resolver ResolveCallback, options ...GenerateOption) (*Data, error) {
	return generate(func(g *generator) FileProvider {
		return NewFSFileProvider(fs, g.fsFileProviderOption...)
	}, resolver, options...)
}

// GenerateDirectory is a version of [Generate] that loads from a directory name.
func GenerateDirectory(rootDir string, resolver ResolveCallback, options ...GenerateOption) (*Data, error) {
	return generate(func(g *generator) FileProvider {
		return NewDirectoryFileProvider(rootDir, g.fsFileProviderOption...)
	}, resolver, options...)
}

func generate(getFileProvider func(g *generator) FileProvider, resolver ResolveCallback, options ...GenerateOption) (*Data, error) {
	g := generator{
		resolver: resolver,
	}
	for _, opt := range options {
		switch xopt := opt.(type) {
		case FSFileProviderOption:
			g.fsFileProviderOption = append(g.fsFileProviderOption, xopt)
		case LoadOption:
			g.loadOptions = append(g.loadOptions, xopt)
		case ResolveOption:
			g.resolveOptions = append(g.resolveOptions, xopt)
		case internalGenerateOption:
			xopt.apply(&g)
		}
	}
	g.fileProvider = getFileProvider(&g)
	return g.generate()
}

// WithGenerateResolveCheck sets whether to check the data using [ResolveCheck]. Default is false.
func WithGenerateResolveCheck(check bool) GenerateOption {
	return fnInternalGenerateOption(func(g *generator) {
		g.resolveCheck = check
	})
}

type generator struct {
	fileProvider FileProvider
	resolver     ResolveCallback
	resolveCheck bool

	fsFileProviderOption []FSFileProviderOption
	loadOptions          []LoadOption
	resolveOptions       []ResolveOption
}

func (g generator) generate() (*Data, error) {
	data, err := Load(g.fileProvider, g.loadOptions...)
	if err != nil {
		return nil, err
	}

	if g.resolveCheck {
		err = ResolveCheck(data, g.resolveOptions...)
		if err != nil {
			return nil, err
		}
	}

	return Resolve(data, g.resolver, g.resolveOptions...)
}
