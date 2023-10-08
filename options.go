package debefix

type FSFileProviderOption interface {
	GenerateOption
	apply(*fsFileProvider)
}

type LoadOption interface {
	GenerateOption
	apply(*loader)
}

type ResolveOption interface {
	GenerateOption
	apply(*resolver)
}

type internalGenerateOption interface {
	GenerateOption
	apply(*generator)
}

type GenerateOption interface {
	isGenerateOption()
}

type IsGenerateOption struct {
}

func (o IsGenerateOption) isGenerateOption() {}

type fnFSFileProviderOption func(item *fsFileProvider)

func (o fnFSFileProviderOption) apply(item *fsFileProvider) {
	o(item)
}

func (o fnFSFileProviderOption) isGenerateOption() {}

type fnLoadOption func(item *loader)

func (o fnLoadOption) apply(item *loader) {
	o(item)
}

func (o fnLoadOption) isGenerateOption() {}

type fnResolveOption func(item *resolver)

func (o fnResolveOption) apply(item *resolver) {
	o(item)
}

func (o fnResolveOption) isGenerateOption() {}

type fnInternalGenerateOption func(item *generator)

func (o fnInternalGenerateOption) apply(item *generator) {
	o.apply(item)
}

func (o fnInternalGenerateOption) isGenerateOption() {}
