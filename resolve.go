package debefix

import (
	cmp2 "cmp"
	"context"
	"errors"
	"strings"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/rrgmc/debefix/v2/internal/external/depgraph"
)

type ResolveType int

const (
	ResolveTypeAdd ResolveType = iota
	ResolveTypeUpdate
)

// ResolveInfo is a context for resolve callbacks.
type ResolveInfo struct {
	Type            ResolveType // type of the resolve (add, update).
	TableID         TableID     // table being resolved.
	UpdateKeyFields []string    // if type is update, the names of the key fields to be used to update.
}

// ResolveCallback is a callback used to resolve ResolveValue values.
type ResolveCallback func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error

// ResolvedCallback is called for each resolved row.
type ResolvedCallback func(ctx context.Context, resolvedData *ResolvedData, resolveInfo ResolveInfo, resolvedRow *Row) error

// Resolve resolves Value and ValueMultiple field values for all rows in "data", using a dependency graph to make
// sure tables are resolved in dependency order.
func Resolve(ctx context.Context, data *Data, resolveFunc ResolveCallback, options ...ResolveOption) (*ResolvedData, error) {
	var optns resolveOptions
	for _, opt := range options {
		opt(&optns)
	}

	// start all processes
	var err error
	for _, process := range optns.processes {
		ctx, err = process.Start(ctx)
		if err != nil {
			return nil, err
		}
	}

	resolvedData := NewResolvedData()

	// build table dependency graph
	depg := depgraph.New()

	for _, table := range data.Tables {
		err := depg.DependOn(table.TableID.TableID(), "") // add blank so tables without dependencies are also returned
		if err != nil {
			return nil, NewResolveErrorf("error build table dependency graph: %w", err)
		}
		for _, dep := range table.Depends {
			if table.TableID.TableID() == dep.TableID() {
				continue
			}
			err = depg.DependOn(table.TableID.TableID(), dep.TableID())
			if err != nil {
				return nil, NewResolveErrorf("error build table dependency graph: %w", err)
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

	if len(tableIDOrder) != len(data.Tables) {
		return nil, NewResolveErrorf("internal error: expected to resolve %d tables but dependency graph returned only %d",
			len(data.Tables), len(tableIDOrder))
	}

	resolvedData.TableOrder = tableIDOrder

	// resolve table's rows in dependency order
	for _, tableID := range tableIDOrder {
		table, ok := data.Tables[tableID]
		if !ok {
			return nil, NewResolveErrorf("tableID not found: %s", tableID)
		}

		for _, row := range table.Rows {
			resolveInfo := ResolveInfo{
				Type:    ResolveTypeAdd,
				TableID: table.TableID,
			}

			// resolve the fields of this row
			resolvedFields, err := resolveRow(ctx, resolvedData, resolveInfo, resolveFunc, row)
			if err != nil {
				return nil, err
			}

			// store resolved table row
			if _, ok := resolvedData.Tables[tableID]; !ok {
				resolvedData.Tables[tableID] = &Table{
					TableID: table.TableID,
				}
			}
			resolvedRow := &Row{
				InternalID:        row.InternalID,
				RefID:             row.RefID,
				Values:            resolvedFields,
				ResolvedCallbacks: row.ResolvedCallbacks,
			}
			resolvedData.Tables[tableID].Rows = append(resolvedData.Tables[tableID].Rows, resolvedRow)

			// call all row callbacks
			for _, rowcb := range row.ResolvedCallbacks {
				err = rowcb(ctx, resolvedData, resolveInfo, resolvedRow)
				if err != nil {
					return nil, err
				}
			}

			// resolve updates
			for _, update := range row.Updates {
				err := resolveUpdate(ctx, resolvedData, resolveFunc, update)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// global updates, done after all data was added
	for _, update := range data.Updates {
		err := resolveUpdate(ctx, resolvedData, resolveFunc, update)
		if err != nil {
			return nil, err
		}
	}

	// finish all processes
	for _, process := range optns.processes {
		err = process.Finish(ctx)
		if err != nil {
			return nil, err
		}
	}

	return resolvedData, nil
}

// resolveUpdate resolves updated requests.
func resolveUpdate(ctx context.Context, resolvedData *ResolvedData, resolveFunc ResolveCallback, update Update) error {
	updateData, err := update.Query.Rows(ctx, resolvedData)
	if err != nil {
		return NewResolveErrorf("error finding rows to update: %w", err)
	}

	for _, ud := range updateData {
		err := update.Action.UpdateRow(ctx, resolvedData, ud.TableID, ud.Row)
		if err != nil {
			return NewResolveErrorf("error updating row: %w", err)
		}
		resolveInfo := ResolveInfo{
			Type:            ResolveTypeUpdate,
			TableID:         ud.TableID,
			UpdateKeyFields: ud.KeyFields,
		}
		resolvedFields, err := resolveRow(ctx, resolvedData, resolveInfo, resolveFunc, ud.Row)
		if err != nil {
			return err
		}
		ud.Row.Values = resolvedFields

		for _, rowcb := range ud.Row.ResolvedCallbacks {
			err = rowcb(ctx, resolvedData, resolveInfo, ud.Row)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// resolveRow resolves one row.
func resolveRow(ctx context.Context, resolvedData *ResolvedData, resolveInfo ResolveInfo, resolveFunc ResolveCallback, row *Row) (ValuesMutable, error) {
	resolvedFields, err := resolveRowValues(ctx, resolvedData, resolveInfo.TableID, row)
	if err != nil {
		return nil, err
	}
	err = resolveRowCallback(ctx, resolveInfo, resolveFunc, resolvedFields)
	if err != nil {
		return nil, err
	}
	return resolvedFields, nil
}

// resolveRowCallback handles the resolve callback.
func resolveRowCallback(ctx context.Context, resolveInfo ResolveInfo,
	resolveFunc ResolveCallback, resolvedFields ValuesMutable) error {
	// call the resolve callback
	err := resolveFunc(ctx, resolveInfo, resolvedFields)
	if err != nil {
		return NewResolveErrorf("error resolving table '%s' row: %w", resolveInfo.TableID.TableID(), err)
	}

	for fieldName, fieldValue := range resolvedFields.All {
		if _, ok := fieldValue.(ResolveValue); ok {
			return NewResolveErrorf("value for table '%s' field '%s' was not resolved", resolveInfo.TableID.TableID(), fieldName)
		}
	}

	return nil
}

// resolveRowValues resolves the values of a single row.
func resolveRowValues(ctx context.Context, resolvedData *ResolvedData, tableID TableID, row *Row) (ValuesMutable, error) {
	// build the fields to send to the callback.
	// load the raw values first, so value loaders may use them.
	resolvedFields := MapValues{}
	for fieldName, fieldValue := range row.Values.All {
		switch fieldValue.(type) {
		case Value, ValueMultiple:
		case IsNotAValue:
			return nil, NewResolveErrorf("value for table '%s' field '%s' should not be used as a field value (type %T)",
				tableID, fieldName, fieldValue)
		default:
			resolvedFields[fieldName] = fieldValue
		}
	}

	// resolvers may return [ResolveLater], do a loop until all fields are resolved, or a limit was reached
	resolveLaterCount := 0
	maxResolveLater := row.Values.Len() * 3
	var resolveLater []string
	for {
		var currentResolveLater []string
		for fieldName, fieldValue := range row.Values.All {
			switch vv := fieldValue.(type) {
			case Value:
				value, ok, err := vv.ResolveValue(ctx, resolvedData, resolvedFields)
				if errors.Is(err, ResolveLater) {
					currentResolveLater = append(currentResolveLater, fieldName)
					continue
				}
				if err != nil {
					return nil, NewResolveErrorf("error resolving table '%s' field '%s': %w", tableID.TableID(), fieldName, err)
				}
				if ok {
					resolvedFields[fieldName] = value
				}
			case ValueMultiple:
				err := vv.Resolve(ctx, resolvedData, tableID, fieldName, resolvedFields)
				if errors.Is(err, ResolveLater) {
					currentResolveLater = append(currentResolveLater, fieldName)
					continue
				}
				if err != nil {
					return nil, NewResolveErrorf("error resolving table '%s' field '%s': %w", tableID.TableID(), fieldName, err)
				}
			}
		}
		if len(currentResolveLater) == 0 {
			break
		}
		if cmp.Equal(currentResolveLater, resolveLater, cmpopts.SortSlices(cmp2.Less[string])) {
			return nil, NewResolveErrorf("could not resolve dependencies for table '%s' fields '%s'",
				tableID.TableID(), strings.Join(currentResolveLater, ", "))
		}
		resolveLaterCount++
		if resolveLaterCount > maxResolveLater {
			return nil, NewResolveErrorf("could not resolve dependencies for table '%s' fields '%s' (max tries reached)",
				tableID.TableID(), strings.Join(currentResolveLater, ", "))
		}
		resolveLater = currentResolveLater
	}

	return resolvedFields, nil
}

type ResolveOption func(options *resolveOptions)

// WithResolveOptionProcess adds a Process to the resolver.
func WithResolveOptionProcess(process Process) ResolveOption {
	return func(options *resolveOptions) {
		options.processes = append(options.processes, process)
	}
}

type resolveOptions struct {
	processes []Process
}

var (
	ResolveRowsStop         = errors.New("stop resolve")
	ResolveNoRows           = errors.New("no rows found")
	ResolveUnknownFieldName = errors.New("unknown field name")
	ResolveLater            = errors.New("resolve later")
)

// ResolveCheck checks if all dependencies between rows are resolvable.
func ResolveCheck(ctx context.Context, data *Data, options ...ResolveOption) error {
	_, err := Resolve(ctx, data, ResolveCheckCallback, options...)
	return err
}

// ResolveCheckCallback is the callback for the ResolveCheck function.
func ResolveCheckCallback(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
	for fn, fv := range values.All {
		if fresolve, ok := fv.(ResolveValue); ok {
			frv, err := fresolve.ResolveValueParse(ctx, uuid.New())
			if err != nil {
				return err
			}
			values.Set(fn, frv)
		}
	}
	return nil
}
