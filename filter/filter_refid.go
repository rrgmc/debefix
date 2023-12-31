package filter

import "github.com/rrgmc/debefix"

type FilterDataRefIDItem[T any] struct {
	Index int
	Data  T
}

type FilterDataRefIDResult[T any] struct {
	Data          []T
	DataRefID     map[string]int
	MissingRefIDs []int
}

func (r FilterDataRefIDResult[T]) RefID(refid string) (bool, T) {
	if idx, ok := r.DataRefID[refid]; ok {
		return true, r.Data[idx]
	}
	var ret T
	return false, ret
}

// FilterDataRefID uses [FilterDataRows] to filter rows and in addition to returning the data, returns a map
// indexed by RefID of the items.
func FilterDataRefID[T any](data *debefix.Data, tableID string, f func(row debefix.Row) (T, error),
	sortCompare func(FilterItem[T], FilterItem[T]) int, options ...FilterDataOption) (FilterDataRefIDResult[T], error) {
	items, err := FilterDataRows[T](data, tableID, f, sortCompare, options...)
	if err != nil {
		return FilterDataRefIDResult[T]{}, err
	}

	ret := FilterDataRefIDResult[T]{
		DataRefID: map[string]int{},
	}
	for idx, item := range items {
		ret.Data = append(ret.Data, item.Item)
		if item.Row.Config.RefID != "" {
			ret.DataRefID[item.Row.Config.RefID] = idx
		} else {
			ret.MissingRefIDs = append(ret.MissingRefIDs, idx)
		}
	}

	return ret, nil
}
