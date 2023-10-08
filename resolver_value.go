package debefix

// ResolveValue indicates a field value that must be resolved.
type ResolveValue interface {
	isResolveValue()
}

// ResolveGenerate is a [ResolveValue] that indicates a value will be generated and must be returned.
type ResolveGenerate struct {
}

func (r ResolveGenerate) isResolveValue() {}

// ResolveContext is the context used to resolve values.
type ResolveContext interface {
	TableID() string
	TableName() string
	ResolveField(fieldName string, value any)
}

type defaultResolveContext struct {
	tableID, tableName string
	resolved           map[string]any
}

func (d *defaultResolveContext) TableID() string {
	return d.tableID
}

func (d *defaultResolveContext) TableName() string {
	return d.tableName
}

func (d *defaultResolveContext) ResolveField(fieldName string, value any) {
	if d.resolved == nil {
		d.resolved = map[string]any{}
	}
	d.resolved[fieldName] = value
}
