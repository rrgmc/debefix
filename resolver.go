package debefix

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
		opt.apply(r)
	}
	return r.resolve(f)
}

// ResolveCheck checks if all dependencies between rows are resolvable.
func ResolveCheck(data *Data, options ...ResolveOption) error {
	return Resolve(data, ResolveCheckCallback, options...)
}

type ResolveCallback func(ctx ResolveContext, fields map[string]any) error

// WithResolveTags set [Resolve] to only resolve rows that contains at least one of these tags. If nil or 0 length,
// no row filtering is performed.
func WithResolveTags(tags []string) ResolveOption {
	return fnResolveOption(func(r *resolver) {
		r.includeTagsFunc = DefaultResolveIncludeTagFunc(tags)
	})
}

// ResolveIncludeTagsFunc is the function signature for [WithResolveTagsFunc]
type ResolveIncludeTagsFunc func(tableID string, rowTags []string) bool

// WithResolveProgress sets a function to receive resolve progress.
func WithResolveProgress(progress func(tableID, tableName string)) ResolveOption {
	return fnResolveOption(func(r *resolver) {
		r.progress = progress
	})
}

// WithResolveRowProgress sets a function to receive resolve row progress.
func WithResolveRowProgress(rowProgress func(tableID, tableName string, current, amount int, isIncluded bool)) ResolveOption {
	return fnResolveOption(func(r *resolver) {
		r.rowProgress = rowProgress
	})
}

// WithResolveTagsFunc sets a row tag filter function.
func WithResolveTagsFunc(f ResolveIncludeTagsFunc) ResolveOption {
	return fnResolveOption(func(r *resolver) {
		r.includeTagsFunc = f
	})
}

// WithReturnResolved sets a callback to get the resolved data.
func WithReturnResolved(f func(resolvedData *Data)) ResolveOption {
	return fnResolveOption(func(r *resolver) {
		r.returnResolved = f
	})
}

// DefaultResolveIncludeTagFunc returns a [ResolveIncludeTagsFunc] check checks if at least one tags is contained.
func DefaultResolveIncludeTagFunc(tags []string) ResolveIncludeTagsFunc {
	return func(tableID string, rowTags []string) bool {
		if len(tags) > 0 && !slices.ContainsFunc(rowTags, func(s string) bool {
			return slices.Contains(tags, s)
		}) {
			return false
		}
		return true
	}
}

type resolver struct {
	data            *Data
	progress        func(tableID, tableName string)
	rowProgress     func(tableID, tableName string, current, amount int, isIncluded bool)
	includeTagsFunc ResolveIncludeTagsFunc
	returnResolved  func(resolvedData *Data)

	// tableData stores the already-parsed row's data.
	resolvedData *Data
}

func (r *resolver) resolve(f ResolveCallback) error {
	// build table dependency graph
	depg := depgraph.New()

	for tableID, table := range r.data.Tables {
		err := depg.DependOn(tableID, "") // add blank so tables without dependencies are also returned
		if err != nil {
			return errors.Join(ResolveError, fmt.Errorf("error build table dependency graph: %w", err))
		}
		for _, dep := range table.Config.Depends {
			if tableID == dep {
				continue
			}
			err = depg.DependOn(tableID, dep)
			if err != nil {
				return errors.Join(ResolveError, fmt.Errorf("error build table dependency graph: %w", err))
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
		table, ok := r.data.Tables[tableID]
		if !ok {
			return errors.Join(ResolveError, fmt.Errorf("tableID not found: %s", tableID))
		}
		tableName := table.Config.TableName
		if tableName == "" {
			tableName = tableID
		}

		if r.progress != nil {
			r.progress(table.ID, tableName)
		}

		for rowIdx, row := range table.Rows {
			isIncluded := r.includeTag(table.ID, row.Config.Tags)

			if r.rowProgress != nil {
				r.rowProgress(table.ID, tableName, rowIdx+1, len(table.Rows), isIncluded)
			}

			if !isIncluded {
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
						return errors.Join(ResolveCallbackError,
							fmt.Errorf("field %s for table %s was not resolved", fieldName, table.ID))
					}
				} else {
					saveFields[fieldName] = fieldValue
				}
			}

			// store table row in memory
			if r.resolvedData == nil {
				r.resolvedData = &Data{
					Tables: map[string]*Table{},
				}
			}
			if _, ok := r.resolvedData.Tables[table.ID]; !ok {
				r.resolvedData.Tables[table.ID] = &Table{
					ID:     table.ID,
					Config: table.Config,
				}
			}
			r.resolvedData.Tables[table.ID].Rows = append(r.resolvedData.Tables[table.ID].Rows, Row{
				Config:     row.Config,
				InternalID: row.InternalID,
				Fields:     saveFields,
			})
		}
	}

	if r.returnResolved != nil {
		r.returnResolved(r.resolvedData)
	}

	return nil
}

// resolveValue resolves Value fields or returns a ResolveValue instance to be resolved by the callback.
func (r *resolver) resolveValue(value Value) (any, error) {
	switch fv := value.(type) {
	case *ValueGenerated:
		return &ResolveGenerate{}, nil
	case *ValueRefID:
		vrowfield, err := r.resolvedData.WalkTableData(fv.TableID, func(row Row) (bool, any, error) {
			if row.Config.RefID == fv.RefID {
				if rowfield, ok := row.Fields[fv.FieldName]; ok {
					return true, rowfield, nil
				} else {
					return false, nil, fmt.Errorf("could not find field %s in refid table %s", fv.FieldName, fv.TableID)
				}
			}
			return false, nil, nil
		})
		if err != nil {
			return nil, errors.Join(ResolveValueError, fmt.Errorf("could not find refid %s in table %s: %w", fv.RefID, fv.TableID, err))
		}
		return vrowfield, nil
	case *ValueInternalID:
		vrowfield, err := r.data.WalkTableData(fv.TableID, func(row Row) (bool, any, error) {
			if row.InternalID == fv.InternalID {
				if rowfield, ok := row.Fields[fv.FieldName]; ok {
					return true, rowfield, nil
				} else {
					return false, nil, fmt.Errorf("could not find field %s in internalid table %s", fv.FieldName, fv.TableID)
				}
			}
			return false, nil, nil
		})
		if err != nil {
			return nil, errors.Join(ResolveValueError, fmt.Errorf("could not find internalid %s in table %s: %w", fv.InternalID, fv.TableID, err))
		}
		return vrowfield, nil
	default:
		return nil, errors.Join(ResolveValueError, fmt.Errorf("unknown Value field"))
	}
}

// includeTag checks whether the tags match the requested ones.
func (r *resolver) includeTag(tableID string, rowTags []string) bool {
	if r.includeTagsFunc == nil {
		return true
	}

	return r.includeTagsFunc(tableID, rowTags)
}

// ResolveCheckCallback is the callback for the ResolveCheck function.
func ResolveCheckCallback(ctx ResolveContext, fields map[string]any) error {
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
