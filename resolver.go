package debefix_poc2

import (
	"fmt"
	"slices"

	"github.com/RangelReale/debefix-poc2/internal/external/depgraph"
	"github.com/google/uuid"
)

func Resolve(data *Data, f ResolveCallback, options ...ResolveOption) error {
	r := &resolver{data: data}
	for _, opt := range options {
		opt(r)
	}
	return r.resolve(f)
}

func ResolveCheck(data *Data, options ...ResolveOption) error {
	return Resolve(data, resolveCheckCallback, options...)
}

type ResolveCallback func(ctx ResolveContext, fields map[string]any) error

type ResolveOption func(*resolver)

func WithResolveTags(tags []string) ResolveOption {
	return func(r *resolver) {
		r.tags = tags
	}
}

type resolver struct {
	data *Data
	tags []string

	tableData map[string]*resolverTable
}

type resolverTable struct {
	rows []resolverRow
}

type resolverRow struct {
	fields     map[string]any
	id         string
	internalID uuid.UUID
}

func (r *resolver) resolve(f ResolveCallback) error {
	// build table dependency graph
	depg := depgraph.New()

	for tableID, table := range r.data.Tables {
		err := depg.DependOn(tableID, "") // add blank so tables without dependencies are accounted for
		if err != nil {
			return fmt.Errorf("error build table dependency graph: %w", err)
		}
		for _, dep := range table.Config.Depends {
			if tableID == dep {
				continue
			}
			err = depg.DependOn(tableID, dep)
			if err != nil {
				return fmt.Errorf("error build table dependency graph: %w", err)
			}
		}
	}

	var tableOrder []string
	for _, layer := range depg.TopoSortedLayers() {
		for _, layeritem := range layer {
			if layeritem == "" {
				continue
			}
			tableOrder = append(tableOrder, layeritem)
		}
	}

	for _, tableID := range tableOrder {
		table := r.data.Tables[tableID]
		tableName := table.Config.TableName
		if tableName == "" {
			tableName = tableID
		}

		for _, row := range table.Rows {
			if !r.includeTag(row.Config.Tags) {
				continue
			}

			callFields := map[string]any{}
			for fieldName, fieldValue := range row.Fields {
				if fvalue, ok := fieldValue.(Value); ok {
					var err error
					fieldValue, err = r.resolveValue(fvalue)
					if err != nil {
						return fmt.Errorf("error resolving Value for table %s: %w", table.ID, err)
					}
				}
				callFields[fieldName] = fieldValue
			}

			ctx := &defaultResolveContext{
				tableID:   table.ID,
				tableName: tableName,
			}

			err := f(ctx, callFields)
			if err != nil {
				return err
			}

			saveFields := map[string]any{}

			for fieldName, fieldValue := range callFields {
				if _, ok := fieldValue.(ResolveValue); ok {
					if rv, ok := ctx.resolved[fieldName]; ok {
						saveFields[fieldName] = rv
					} else {
						return fmt.Errorf("field %s for table %s was not resolved", fieldName, table.ID)
					}
				} else {
					saveFields[fieldName] = fieldValue
				}
			}

			if r.tableData == nil {
				r.tableData = map[string]*resolverTable{}
			}
			if _, ok := r.tableData[table.ID]; !ok {
				r.tableData[table.ID] = &resolverTable{}
			}
			r.tableData[table.ID].rows = append(r.tableData[table.ID].rows, resolverRow{
				fields:     saveFields,
				id:         row.Config.ID,
				internalID: row.InternalID,
			})
		}
	}

	return nil
}

func (r *resolver) resolveValue(value Value) (any, error) {
	switch fv := value.(type) {
	case *ValueGenerated:
		return &ResolveGenerate{}, nil
	case *ValueRefID:
		vdb, ok := r.tableData[fv.Table]
		if !ok {
			return nil, fmt.Errorf("could not find refid table %s (refid %s)", fv.Table, fv.ID)
		}
		for _, vrow := range vdb.rows {
			if vrow.id == fv.ID {
				if vrowfield, ok := vrow.fields[fv.FieldName]; ok {
					return vrowfield, nil
				} else {
					return nil, fmt.Errorf("could not find field %s in refid table %s", fv.FieldName, fv.Table)
				}
			}
		}
		return nil, fmt.Errorf("could not find refid %s in table %s", fv.ID, fv.Table)
	case *ValueInternalID:
		vdb, ok := r.tableData[fv.Table]
		if !ok {
			return nil, fmt.Errorf("could not find internalid table %s (internalid %s)", fv.Table, fv.InternalID)
		}
		for _, vrow := range vdb.rows {
			if vrow.internalID == fv.InternalID {
				if vrowfield, ok := vrow.fields[fv.FieldName]; ok {
					return vrowfield, nil
				} else {
					return nil, fmt.Errorf("could not find field %s in internalid table %s", fv.FieldName, fv.Table)
				}
			}
		}
		return nil, fmt.Errorf("could not find internalid %s in table %s", fv.InternalID, fv.Table)
	default:
		return nil, fmt.Errorf("unknown Value field")
	}
}

func (r *resolver) includeTag(tags []string) bool {
	if len(r.tags) > 0 && !slices.ContainsFunc(tags, func(s string) bool {
		return slices.Contains(r.tags, s)
	}) {
		return false
	}
	return true
}

func resolveCheckCallback(ctx ResolveContext, fields map[string]any) error {
	for fn, fv := range fields {
		if fresolve, ok := fv.(ResolveValue); ok {
			switch fresolve.(type) {
			case *ResolveGenerate:
				ctx.ResolveField(fn, uuid.New())
			}
		}
	}
	return nil
}
