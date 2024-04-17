package debefix

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Value indicates a field value requires processing.
type Value interface {
	isValue()
}

// ValueRefID is a [Value] that references a field value in a table using the RefID (string ID).
type ValueRefID struct {
	TableID   string
	RefID     string
	FieldName string
}

// TableDepends indicates a dependency on another table.
func (v ValueRefID) TableDepends() string {
	return v.TableID
}

// ValueGenerated is a [Value] that will be generated in the future (possibly by a database).
type ValueGenerated struct {
	Type string
}

// ValueInternalID is a [Value] that references a field value in a table using the internal ID.
type ValueInternalID struct {
	TableID    string
	InternalID uuid.UUID
	FieldName  string
}

// TableDepends indicates a dependency on another table.
func (v ValueInternalID) TableDepends() string {
	return v.TableID
}

func (v ValueRefID) isValue()      {}
func (v ValueGenerated) isValue()  {}
func (v ValueInternalID) isValue() {}

// valueTableDepends is an interface to indicate that a [Value] adds a dependency on another table.
type valueTableDepends interface {
	TableDepends() string
}

// parseValue parses !dbfexpr expressions.
func parseValue(value string, parent parentRowInfo) (Value, error) {
	fields := strings.Split(value, ":")
	if len(fields) == 0 {
		return nil, errors.Join(ValueError, fmt.Errorf("invalid !dbf tag: %s", value))
	}

	switch fields[0] {
	case "refid": // refid:<table>:<refid>:<fieldname>
		if len(fields) != 4 {
			return nil, errors.Join(ValueError, fmt.Errorf("invalid !dbf tag value: %s", value))
		}
		return &ValueRefID{TableID: fields[1], RefID: fields[2], FieldName: fields[3]}, nil
	case "parent": // parent<:level>:<fieldname>
		parentLevel := 1
		fieldName := fields[1]
		if len(fields) == 3 {
			level, err := strconv.ParseInt(fields[1], 10, 32)
			if err != nil {
				return nil, errors.Join(ValueError, fmt.Errorf("invalid level '%s' in parent expression: %w", fields[1], err))
			}
			parentLevel = int(level)
			fieldName = fields[2]
		} else if len(fields) != 2 {
			return nil, errors.Join(ValueError, fmt.Errorf("invalid !dbf tag value: %s", value))
		}
		plevel := parent.ParentLevel(parentLevel)
		if !plevel.ParentSupported() {
			return nil, errors.Join(ValueError, errors.New("parents not supported in current context"))
		}
		if !plevel.HasParent() {
			return nil, errors.Join(ValueError, errors.New("value has no parent"))
		}
		return &ValueInternalID{TableID: plevel.TableID(), InternalID: plevel.InternalID(), FieldName: fieldName}, nil
	case "generated": // generated<:type>
		ret := &ValueGenerated{}
		if len(fields) > 1 {
			ret.Type = fields[1]
		}
		return ret, nil
	default:
		return nil, errors.Join(ValueError, fmt.Errorf("unknown !dbfexpr tag type: %s", value))
	}
}

// parentRowInfo gets parent info from a level number.
type parentRowInfo interface {
	ParentLevel(level int) parentRowInfoData
}

// parentRowInfoData indicates if a parent exists and its information.
type parentRowInfoData interface {
	ParentSupported() bool
	HasParent() bool
	TableID() string
	InternalID() uuid.UUID
}

// noParentRowInfo indicates that no parent exists in the current context.
type noParentRowInfo struct {
}

func (n noParentRowInfo) ParentLevel(level int) parentRowInfoData {
	return &noParentRowInfoData{}
}

// noParentRowInfo indicates that no parent exists in the current context.
type noParentRowInfoData struct {
}

func (n noParentRowInfoData) ParentSupported() bool {
	return true
}

func (n noParentRowInfoData) HasParent() bool {
	return false
}

func (n noParentRowInfoData) TableID() string {
	return ""
}

func (n noParentRowInfoData) InternalID() uuid.UUID {
	return uuid.UUID{}
}

// defaultParentRowInfoData indicates a parent exists in the current context.
type defaultParentRowInfo struct {
	parent parentRowInfo
	data   parentRowInfoData
}

func (n defaultParentRowInfo) ParentLevel(level int) parentRowInfoData {
	if level == 1 {
		return n.data
	}
	if level < 1 || n.parent == nil {
		return noParentRowInfoData{}
	}
	return n.parent.ParentLevel(level - 1)
}

// defaultParentRowInfoData indicates a parent exists in the current context.
type defaultParentRowInfoData struct {
	tableID    string
	internalID uuid.UUID
}

func (n defaultParentRowInfoData) ParentSupported() bool {
	return true
}

func (n defaultParentRowInfoData) HasParent() bool {
	return true
}

func (n defaultParentRowInfoData) TableID() string {
	return n.tableID
}

func (n defaultParentRowInfoData) InternalID() uuid.UUID {
	return n.internalID
}

// unsupportedParentRowInfo indicates parents are not supported in the current context.
type unsupportedParentRowInfo struct {
}

func (n unsupportedParentRowInfo) ParentLevel(level int) parentRowInfoData {
	return &unsupportedParentRowInfoData{}
}

// unsupportedParentRowInfo indicates parents are not supported in the current context.
type unsupportedParentRowInfoData struct {
}

func (n unsupportedParentRowInfoData) ParentSupported() bool {
	return false
}

func (n unsupportedParentRowInfoData) HasParent() bool {
	return false
}

func (n unsupportedParentRowInfoData) TableID() string {
	return ""
}

func (n unsupportedParentRowInfoData) InternalID() uuid.UUID {
	return uuid.UUID{}
}
