package debefix

import "io/fs"

func Generate(fileProvider FileProvider, resolver ResolveCallback, options ...GenerateOption) error {
	return generate(func(g *generator) FileProvider {
		return fileProvider
	}, resolver, options...)
}

func GenerateFS(fs fs.FS, resolver ResolveCallback, options ...GenerateOption) error {
	return generate(func(g *generator) FileProvider {
		return NewFSFileProvider(fs, g.fsFileProviderOption...)
	}, resolver, options...)
}

func GenerateDirectory(rootDir string, resolver ResolveCallback, options ...GenerateOption) error {
	return generate(func(g *generator) FileProvider {
		return NewDirectoryFileProvider(rootDir, g.fsFileProviderOption...)
	}, resolver, options...)
}

func generate(getFileProvider func(g *generator) FileProvider, resolver ResolveCallback, options ...GenerateOption) error {
	g := generator{
		resolver: resolver,
	}
	for _, opt := range options {
		opt(&g)
	}
	g.fileProvider = getFileProvider(&g)
	return g.generate()
}

type GenerateOption func(g *generator)

func WithGenerateResolveCheck(check bool) GenerateOption {
	return func(g *generator) {
		g.resolveCheck = check
	}
}

func WithGenerateFSFileProviderOptions(o ...FSFileProviderOption) GenerateOption {
	return func(g *generator) {
		g.fsFileProviderOption = o
	}
}

func WithGenerateLoadOptions(o ...LoadOption) GenerateOption {
	return func(g *generator) {
		g.loadOptions = o
	}
}

func WithGenerateResolveOptions(o ...ResolveOption) GenerateOption {
	return func(g *generator) {
		g.resolveOptions = o
	}
}

type generator struct {
	fileProvider FileProvider
	resolver     ResolveCallback
	resolveCheck bool

	fsFileProviderOption []FSFileProviderOption
	loadOptions          []LoadOption
	resolveOptions       []ResolveOption
}

func (g generator) generate() error {
	data, err := Load(g.fileProvider, g.loadOptions...)
	if err != nil {
		return err
	}

	if g.resolveCheck {
		err = ResolveCheck(data, g.resolveOptions...)
		if err != nil {
			return err
		}
	}

	return Resolve(data, g.resolver, g.resolveOptions...)
}
