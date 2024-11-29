package debefix

// TableID is a table identification, usually a table name.
// Users of the library may want to add more fields, like a database name.
type TableID interface {
	TableID() string
	TableName() string
}

// TableName implements TableID with a simple table name.
type TableName string

func (t TableName) TableID() string {
	return string(t)
}

func (t TableName) TableName() string {
	return string(t)
}

// TableNameID implement TableID with separate id and table name values.
type TableNameID struct {
	id        string
	tableName string
}

var _ TableID = TableNameID{}

// NewTableNameID implement TableID with separate id and table name values.
func NewTableNameID(id string, tableName string) TableNameID {
	return TableNameID{
		id:        id,
		tableName: tableName,
	}
}

func (t TableNameID) TableID() string {
	return t.id
}

func (t TableNameID) TableName() string {
	return t.tableName
}
