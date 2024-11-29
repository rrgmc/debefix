package debefix

import "slices"

// Table represents a table, its dependencies, and list of rows.
type Table struct {
	TableID TableID
	Depends []TableID
	Rows    []*Row
}

// AddDependencies adds dependencies on another tables.
func (t *Table) AddDependencies(deps ...TableID) {
	for _, dep := range deps {
		if dep.TableID() != t.TableID.TableID() && !slices.ContainsFunc(t.Depends, func(name TableID) bool {
			return dep.TableID() == name.TableID()
		}) {
			t.Depends = append(t.Depends, dep)
		}
	}
}
