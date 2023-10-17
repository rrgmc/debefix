package filter

import (
	"fmt"
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/rrgmc/debefix"
	"gotest.tools/v3/assert/opt"
)

// FilterData returns a filtered set of a single table of [debefix.Data], converting a [debefix.Row] to a
// concrete type using generics.
func FilterData[T any](data *debefix.Data, tableID string, f func(row debefix.Row) (T, error),
	options ...FilterDataOption) ([]T, error) {
	var optns filterDataOptions
	for _, op := range options {
		op(&optns)
	}

	var ret []T
	_, err := data.
		ExtractRows(func(table *debefix.Table, row debefix.Row) (bool, error) {
			if table.ID == tableID {
				include := optns.filterAll

				// filter refID
				if len(optns.filterRefIDs) > 0 {
					include = slices.Contains(optns.filterRefIDs, row.Config.RefID)
				}

				// filter fields
				if len(optns.filterFields) > 0 {
					isFilter := 0
					for filterField, filterValue := range optns.filterFields {
						fieldValue, isField := optns.filterFields[filterField]
						if !isField {
							return false, fmt.Errorf("field '%s' does not exists", filterField)
						}
						if cmp.Equal(filterValue, fieldValue, opt.TimeWithThreshold(time.Hour)) {
							isFilter++
						}
					}
					include = isFilter == len(optns.filterFields)
				}

				// filter func
				if optns.filterRow != nil {
					isRow, err := optns.filterRow(row)
					if err != nil {
						return false, fmt.Errorf("error filtering row: %s", err)
					}
					include = isRow
				}

				if include {
					data, err := f(row)
					if err != nil {
						return false, err
					}
					ret = append(ret, data)
				}
			}
			return true, nil
		})
	if err != nil {
		return nil, fmt.Errorf("error loading data for '%s`: %w", tableID, err)
	}

	return ret, nil
}

type filterDataOptions struct {
	filterAll    bool
	filterRefIDs []string
	filterFields map[string]any
	filterRow    func(row debefix.Row) (bool, error)
}

type FilterDataOption func(*filterDataOptions)

// WithFilterAll include all records by default, depending on other filters if they exist.
// By default, if no filters were set, no record would be returned. Use this to return all rows in this case.
// All requested filters must return true to select the row.
func WithFilterAll(filterAll bool) FilterDataOption {
	return func(o *filterDataOptions) {
		o.filterAll = true
	}
}

// WithFilterRefIDs filters by refID.
// All requested filters must return true to select the row.
func WithFilterRefIDs(refIDs []string) FilterDataOption {
	return func(o *filterDataOptions) {
		o.filterRefIDs = refIDs
	}
}

// WithFilterFields filters fields values.
// All requested filters must return true to select the row.
func WithFilterFields(fields map[string]any) FilterDataOption {
	return func(o *filterDataOptions) {
		o.filterFields = fields
	}
}

// WithFilterRow filters using a callback.
// All requested filters must return true to select the row.
func WithFilterRow(filterRow func(row debefix.Row) (bool, error)) FilterDataOption {
	return func(o *filterDataOptions) {
		o.filterRow = filterRow
	}
}
