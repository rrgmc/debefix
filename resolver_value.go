package debefix_poc2

type ResolveValue interface {
	isResoleValue()
}

type ResolveGenerate struct {
}

func (r ResolveGenerate) isResoleValue() {}

type ResolveContext interface {
	ResolveField(fieldName string, value any)
}

type defaultResolveContext struct {
	resolved map[string]any
}

func (d *defaultResolveContext) ResolveField(fieldName string, value any) {
	if d.resolved == nil {
		d.resolved = map[string]any{}
	}
	d.resolved[fieldName] = value
}
