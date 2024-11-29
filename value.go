package debefix

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
)

// SetValueRefIDData is a special ValueMultiple which sets the RefID for the current row.
// The field containing it is not added to the row.
type SetValueRefIDData struct {
	RefID RefID
}

// SetValueRefID is a special ValueMultiple which sets the RefID for the current row.
// The field containing it is not added to the row.
func SetValueRefID(refID RefID) SetValueRefIDData {
	return SetValueRefIDData{
		RefID: refID,
	}
}

var _ ValueMultiple = SetValueRefIDData{}

func (v SetValueRefIDData) Resolve(ctx context.Context, resolvedData *ResolvedData, tableID TableID, fieldName string, values ValuesMutable) error {
	return NewResolveError("SetValueRefID is not meant to be resolved")
}

// ValueInternalIDData represents a field value from a table row by internal id.
// It implements Value, QueryRow, and can return a UpdateQuery.
type ValueInternalIDData struct {
	TableID    TableID
	InternalID uuid.UUID
	FieldName  string
}

// ValueInternalID represents a field value from a table row by internal id.
// It implements Value, QueryRow, and can return a UpdateQuery.
func ValueInternalID(tableID TableID, internalID uuid.UUID, fieldName string) ValueInternalIDData {
	return ValueInternalIDData{
		TableID:    tableID,
		InternalID: internalID,
		FieldName:  fieldName,
	}
}

var _ Value = (*ValueInternalIDData)(nil)
var _ ValueDependencies = (*ValueInternalIDData)(nil)
var _ QueryRow = (*ValueInternalIDData)(nil)

func (v ValueInternalIDData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	rv, err := resolvedData.FindInternalIDValue(v)
	if err != nil {
		return nil, false, err
	}
	return rv, true, nil
}

func (v ValueInternalIDData) TableDependencies() []TableID {
	return []TableID{v.TableID}
}

func (v ValueInternalIDData) QueryRow(data *Data) (QueryRowResult, error) {
	row, err := data.FindInternalIDRow(v.TableID, v.InternalID)
	if err != nil {
		return QueryRowResult{}, err
	}
	return QueryRowResult{TableID: v.TableID, Row: row}, nil
}

func (v ValueInternalIDData) UpdateQuery(keyFields []string) UpdateQuery {
	return UpdateQueryRow(v, keyFields)
}

// ValueRefIDData represents a field value from a table row by its RefID.
// It implements Value, QueryRow, and can return a UpdateQuery.
type ValueRefIDData struct {
	TableID   TableID
	RefID     RefID
	FieldName string
}

// ValueRefID represents a field value from a table row by its RefID.
// It implements Value, QueryRow, and can return a UpdateQuery.
func ValueRefID(tableID TableID, refID RefID, fieldName string) ValueRefIDData {
	return ValueRefIDData{
		TableID:   tableID,
		RefID:     refID,
		FieldName: fieldName,
	}
}

var _ Value = (*ValueRefIDData)(nil)
var _ ValueDependencies = (*ValueRefIDData)(nil)
var _ QueryRow = (*ValueRefIDData)(nil)

func (v ValueRefIDData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	value, err := resolvedData.FindRefIDRowValue(v)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (v ValueRefIDData) TableDependencies() []TableID {
	return []TableID{v.TableID}
}

func (v ValueRefIDData) QueryRow(data *Data) (QueryRowResult, error) {
	row, err := data.FindRefIDRow(v.TableID, v.RefID)
	if err != nil {
		return QueryRowResult{}, err
	}
	return QueryRowResult{TableID: v.TableID, Row: row}, nil
}

func (v ValueRefIDData) UpdateQuery(keyFields []string) UpdateQuery {
	return UpdateQueryRow(v, keyFields)
}

// ValueBaseTimeAddData is a Value that calculates a date based on the Data base time.
type ValueBaseTimeAddData struct {
	AddDays    int
	AddHours   int
	AddMinutes int
	AddSeconds int
}

// ValueBaseTimeAdd is a Value that calculates a date based on the Data base time.
func ValueBaseTimeAdd(options ...ValueBaseTimeAddDataOption) ValueBaseTimeAddData {
	ret := ValueBaseTimeAddData{}
	for _, option := range options {
		option(&ret)
	}
	return ret
}

var _ Value = (*ValueBaseTimeAddData)(nil)

type ValueBaseTimeAddDataOption func(*ValueBaseTimeAddData)

func WithAddDate(days, hours, minutes, seconds int) ValueBaseTimeAddDataOption {
	return func(v *ValueBaseTimeAddData) {
		v.AddDays = days
		v.AddHours = hours
		v.AddMinutes = minutes
		v.AddSeconds = seconds
	}
}

func WithAddTime(hours, minutes, seconds int) ValueBaseTimeAddDataOption {
	return func(v *ValueBaseTimeAddData) {
		v.AddHours = hours
		v.AddMinutes = minutes
		v.AddSeconds = seconds
	}
}

func WithAddDays(days int) ValueBaseTimeAddDataOption {
	return func(v *ValueBaseTimeAddData) {
		v.AddDays = days
	}
}

func WithAddHours(hours int) ValueBaseTimeAddDataOption {
	return func(v *ValueBaseTimeAddData) {
		v.AddHours = hours
	}
}

func WithAddMinutes(minutes int) ValueBaseTimeAddDataOption {
	return func(v *ValueBaseTimeAddData) {
		v.AddMinutes = minutes
	}
}

func WithAddSeconds(seconds int) ValueBaseTimeAddDataOption {
	return func(v *ValueBaseTimeAddData) {
		v.AddSeconds = seconds
	}
}

func (v ValueBaseTimeAddData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	t := resolvedData.BaseTime.Add((time.Hour * time.Duration(v.AddHours)) + (time.Minute * time.Duration(v.AddMinutes)) + (time.Second * time.Duration(v.AddSeconds)))
	if v.AddDays > 0 {
		t = t.AddDate(0, 0, v.AddDays)
	}
	return t, true, nil
}

// ValueFormatData is a Value that formats a string based on other field's values.
type ValueFormatData struct {
	Format string
	Args   []any
}

// ValueFormat is a Value that formats a string based on other field's values.
func ValueFormat(format string, args ...any) ValueFormatData {
	return ValueFormatData{
		Format: format,
		Args:   args,
	}
}

var _ Value = (*ValueFormatData)(nil)
var _ ValueDependencies = (*ValueFormatData)(nil)

func (v ValueFormatData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	fmtArgs, argOk, err := resolvedData.ResolveArgs(ctx, values, v.Args...)
	if err != nil {
		return nil, false, err
	}
	if !argOk {
		return nil, false, ResolveLater
	}
	return fmt.Sprintf(v.Format, fmtArgs...), true, nil
}

func (v ValueFormatData) TableDependencies() []TableID {
	var deps []TableID
	for _, arg := range v.Args {
		if vd, ok := arg.(ValueDependencies); ok {
			deps = append(deps, vd.TableDependencies()...)
		}
	}
	return deps
}

// ValueFormatTemplateData is a Value that formats a string based on other field's values using "text/template".
type ValueFormatTemplateData struct {
	Template string
	Args     map[string]any
}

// ValueFormatTemplate is a Value that formats a string based on other field's values using "text/template".
func ValueFormatTemplate(template string, args map[string]any) ValueFormatTemplateData {
	return ValueFormatTemplateData{
		Template: template,
		Args:     args,
	}
}

var _ Value = (*ValueFormatTemplateData)(nil)
var _ ValueDependencies = (*ValueFormatTemplateData)(nil)

func (v ValueFormatTemplateData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	fmtArgs, argOk, err := resolvedData.ResolveMapArgs(ctx, values, v.Args)
	if err != nil {
		return nil, false, err
	}
	if !argOk {
		return nil, false, ResolveLater
	}
	tmpl, err := template.New("tmpl").Parse(v.Template)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse template: %w", err)
	}
	var buf bytes.Buffer
	err = tmpl.Execute(&buf, fmtArgs)
	if err != nil {
		return nil, false, fmt.Errorf("failed to execute template: %w", err)
	}
	return buf.String(), true, nil
}

func (v ValueFormatTemplateData) TableDependencies() []TableID {
	var deps []TableID
	for _, arg := range v.Args {
		if vd, ok := arg.(ValueDependencies); ok {
			deps = append(deps, vd.TableDependencies()...)
		}
	}
	return deps
}

// ValueFieldValueData is a Value which returns the value of a field of the current row.
type ValueFieldValueData struct {
	FieldName string
}

func ValueFieldValue(fieldName string) ValueFieldValueData {
	return ValueFieldValueData{
		FieldName: fieldName,
	}
}

var _ Value = (*ValueFieldValueData)(nil)

func (d ValueFieldValueData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	value, ok := values.Get(d.FieldName)
	if !ok {
		return nil, false, ResolveLater
	}
	return value, true, nil
}

// ValueRefFieldValueData is a Value which returns the value of a field of a row of the passed table where one of the
// other row's field value is equal to one of the current row's field value.
type ValueRefFieldValueData struct {
	SourceFieldName  string  // the field name of the current row to compare for equality.
	TableID          TableID // the table ID of the other table.
	CompareFieldName string  // the field name of the other table row (from TableID) to compare for equality.
	ReturnFieldName  string  // the field name of the other table row to have its value returned.
}

// ValueRefFieldValue is a Value which returns the value of a field of a row of the passed table where one of the
// other row's field value is equal to one of the current row's field value.
func ValueRefFieldValue(sourceFieldName string, tableID TableID, compareFieldName string,
	returnFieldName string) ValueRefFieldValueData {
	return ValueRefFieldValueData{
		SourceFieldName:  sourceFieldName,
		TableID:          tableID,
		CompareFieldName: compareFieldName,
		ReturnFieldName:  returnFieldName,
	}
}

var _ Value = (*ValueRefFieldValueData)(nil)
var _ ValueDependencies = (*ValueRefFieldValueData)(nil)

func (d ValueRefFieldValueData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	sourceValue, ok := values.Get(d.SourceFieldName)
	if !ok {
		return nil, false, nil
	}
	resolveRow, err := resolvedData.FindTableRow(d.TableID, func(innerRow *Row) (bool, error) {
		ivalue, ok := innerRow.Values.Get(d.CompareFieldName)
		if ok {
			if cmp.Equal(sourceValue, ivalue) {
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, false, err
	}

	value, ok := resolveRow.Values.Get(d.ReturnFieldName)
	if !ok {
		return nil, false, nil
	}
	return value, true, nil
}

func (d ValueRefFieldValueData) TableDependencies() []TableID {
	return []TableID{d.TableID}
}

// ValueDefaultData returns the DefaultValue if the other Value don't return a valid value (returns "false" for the 2nd
// result value).
type ValueDefaultData struct {
	Value        Value
	DefaultValue any
}

// ValueDefault returns the DefaultValue if the other Value don't return a valid value (returns "false" for the 2nd
// result value).
func ValueDefault(value Value, defaultValue any) ValueDefaultData {
	return ValueDefaultData{
		Value:        value,
		DefaultValue: defaultValue,
	}
}

var _ Value = (*ValueDefaultData)(nil)
var _ ValueDependencies = (*ValueDefaultData)(nil)

func (d ValueDefaultData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	value, ok, err := d.Value.ResolveValue(ctx, resolvedData, values)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return d.DefaultValue, true, nil
	}
	return value, true, nil
}

func (d ValueDefaultData) TableDependencies() []TableID {
	if td, ok := d.Value.(ValueDependencies); ok {
		return td.TableDependencies()
	}
	return nil
}

// ValueFormatFuncData resolves Value, and allows it to be changed on a callback formatting function.
type ValueFormatFuncData struct {
	Value      Value
	FormatFunc func(any) (any, error)
}

// ValueFormatFunc resolves Value, and allows it to be changed on a callback formatting function.
func ValueFormatFunc(value Value, formatFunc func(any) (any, error)) ValueFormatFuncData {
	return ValueFormatFuncData{
		Value:      value,
		FormatFunc: formatFunc,
	}
}

var _ Value = (*ValueFormatFuncData)(nil)
var _ ValueDependencies = (*ValueFormatFuncData)(nil)

func (d ValueFormatFuncData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	value, ok, err := d.Value.ResolveValue(ctx, resolvedData, values)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}
	newValue, err := d.FormatFunc(value)
	if err != nil {
		return nil, false, err
	}
	return newValue, true, nil
}

func (d ValueFormatFuncData) TableDependencies() []TableID {
	if td, ok := d.Value.(ValueDependencies); ok {
		return td.TableDependencies()
	}
	return nil
}
