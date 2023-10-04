package debefix_poc2

import (
	"errors"
	"fmt"
	"slices"

	"github.com/RangelReale/debefix/internal/external/depgraph"
	"github.com/google/uuid"
)

// Resolve calls a callback for each table row, taking table dependency in account.
func Resolve(data *Data, f ResolveCallback, options ...ResolveOption) error {
	r := &resolver{data: data}
	for _, opt := range options {
		opt(r)
	}
	return r.resolve(f)
}

// ResolveCheck checks if all dependencies between rows are resolvable.
func ResolveCheck(data *Data, options ...ResolveOption) error {
	return Resolve(data, resolveCheckCallback, options...)
}

type ResolveCallback func(ctx ResolveContext, fields map[string]any) error

type ResolveOption func(*resolver)

// WithResolveTags set Resolve to only resolve rows that contains at least one of these tags. If nil or 0 length,
// no row filtering is performed.
func WithResolveTags(tags []string) ResolveOption {
	return func(r *resolver) {
		r.tags = tags
	}
}

type resolver struct {
	data *Data
	tags []string

	// tableData stores the already-parsed row's data.
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
		err := depg.DependOn(tableID, "") // add blank so tables without dependencies are also returned
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

	var tableIDOrder []string
	for _, layer := range depg.TopoSortedLayers() {
		for _, layeritem := range layer {
			if layeritem == "" {
				continue
			}
			tableIDOrder = append(tableIDOrder, layeritem)
		}
	}

	for _, tableID := range tableIDOrder {
		table := r.data.Tables[tableID]
		tableName := table.Config.TableName
		if tableName == "" {
			tableName = tableID
		}

		for _, row := range table.Rows {
			if !r.includeTag(row.Config.Tags) {
				continue
			}

			// build the fields to send to the callback
			callFields := map[string]any{}
			for fieldName, fieldValue := range row.Fields {
				// Value fields must be resolved by a ResolveValue.
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

			// build the row to save in memory
			saveFields := map[string]any{}

			for fieldName, fieldValue := range callFields {
				// check if all ResolveValue fields were resolved.
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

			// store table row in memory
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

// resolveValue resolves Value fields or returns a ResolveValue instance to be resolved by the callback.
func (r *resolver) resolveValue(value Value) (any, error) {
	switch fv := value.(type) {
	case *ValueGenerated:
		return &ResolveGenerate{}, nil
	case *ValueRefID:
		vrowfield, err := r.walkTableData(fv.TableID, func(row resolverRow) (bool, any, error) {
			if row.id == fv.ID {
				if rowfield, ok := row.fields[fv.FieldName]; ok {
					return true, rowfield, nil
				} else {
					return false, nil, fmt.Errorf("could not find field %s in refid table %s", fv.FieldName, fv.TableID)
				}
			}
			return false, nil, nil
		})
		if err != nil {
			return nil, fmt.Errorf("could not find refid %s in table %s: %w", fv.ID, fv.TableID, err)
		}
		return vrowfield, nil
	case *ValueInternalID:
		vrowfield, err := r.walkTableData(fv.TableID, func(row resolverRow) (bool, any, error) {
			if row.internalID == fv.InternalID {
				if rowfield, ok := row.fields[fv.FieldName]; ok {
					return true, rowfield, nil
				} else {
					return false, nil, fmt.Errorf("could not find field %s in internalid table %s", fv.FieldName, fv.TableID)
				}
			}
			return false, nil, nil
		})
		if err != nil {
			return nil, fmt.Errorf("could not find internalid %s in table %s: %w", fv.InternalID, fv.TableID, err)
		}
		return vrowfield, nil
	default:
		return nil, fmt.Errorf("unknown Value field")
	}
}

// includeTag checks whether the tags match the requested ones.
func (r *resolver) includeTag(tags []string) bool {
	if len(r.tags) > 0 && !slices.ContainsFunc(tags, func(s string) bool {
		return slices.Contains(r.tags, s)
	}) {
		return false
	}
	return true
}

// walkTableData searches for rows in tables.
func (r *resolver) walkTableData(tableID string, f func(row resolverRow) (bool, any, error)) (any, error) {
	vdb, ok := r.tableData[tableID]
	if !ok {
		return nil, fmt.Errorf("could not find table %s", tableID)
	}
	for _, vrow := range vdb.rows {
		ok, v, err := f(vrow)
		if err != nil {
			return nil, err
		}
		if ok {
			return v, nil
		}
	}

	return errors.New("row not found in data"), nil
}

// resolveCheckCallback is the callback for the ResolveCheck function.
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
