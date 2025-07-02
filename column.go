package sq

import (
	"context"
	"fmt"
	"time"
)

// ColumnMapper defines column mapper function.
type ColumnMapper func(context.Context, *Column)

// Column keeps track of what the values mapped to what Field in an
// InsertQuery or SelectQuery.
type Column struct {
	dialect string
	// determines if UPDATE or INSERT
	isUpdate bool
	// UPDATE
	assignments Assignments
	// INSERT
	rowStarted    bool
	rowEnded      bool
	firstField    string
	insertColumns Fields
	rowValues     RowValues
}

// Set maps the value to the Field.
func (col *Column) set(field Field, value any) {
	if field == nil {
		panic(fmt.Errorf("%s", callsite(1)+"setting a nil field"))
	}
	// UPDATE mode
	if col.isUpdate {
		col.assignments = append(col.assignments, Set(field, value))
		return
	}
	// INSERT mode
	name := toString(col.dialect, field)
	if name == "" {
		panic(fmt.Errorf("%s", callsite(1)+"field name is empty"))
	}
	if !col.rowStarted {
		col.rowStarted = true
		col.firstField = name
		col.insertColumns = append(col.insertColumns, field)
		col.rowValues = append(col.rowValues, RowValue{value})
		return
	}
	if col.rowStarted && name == col.firstField {
		if !col.rowEnded {
			col.rowEnded = true
		}
		// Start a new RowValue
		col.rowValues = append(col.rowValues, RowValue{value})
		return
	}
	if !col.rowEnded {
		col.insertColumns = append(col.insertColumns, field)
	}
	// Append to last RowValue
	last := len(col.rowValues) - 1
	col.rowValues[last] = append(col.rowValues[last], value)
}

// Set maps any value to the field.
func (col *Column) Set(field Field, value any) { col.set(field, value) }

// SetBytes maps the []byte value to the field.
func (col *Column) SetBytes(field Binary, value []byte) { col.set(field, value) }

// SetBool maps the bool value to the field.
func (col *Column) SetBool(field Boolean, value bool) { col.set(field, value) }

// SetFloat32 maps the float32 value to the field.
func (col *Column) SetFloat32(field Number, value float32) { col.set(field, value) }

func (col *Column) SetFloat64(field Number, value float64) { col.set(field, value) }

// SetUint maps the uint value to the field.
func (col *Column) SetUint(field Number, value uint) { col.set(field, value) }

// SetUint8 maps the int value to the field.
func (col *Column) SetUint8(field Number, value uint8) { col.set(field, value) }

// SetUint16 maps the int value to the field.
func (col *Column) SetUint16(field Number, value uint16) { col.set(field, value) }

// SetUint32 maps the int value to the field.
func (col *Column) SetUint32(field Number, value uint32) { col.set(field, value) }

// SetUint64 maps the uint64 value to the field.
func (col *Column) SetUint64(field Number, value uint64) { col.set(field, value) }

// SetInt maps the int value to the field.
func (col *Column) SetInt(field Number, value int) { col.set(field, value) }

// SetInt8 maps the int value to the field.
func (col *Column) SetInt8(field Number, value int8) { col.set(field, value) }

// SetInt16 maps the int value to the field.
func (col *Column) SetInt16(field Number, value int16) { col.set(field, value) }

// SetInt32 maps the int value to the field.
func (col *Column) SetInt32(field Number, value int32) { col.set(field, value) }

// SetInt64 maps the int64 value to the field.
func (col *Column) SetInt64(field Number, value int64) { col.set(field, value) }

// SetString maps the string value to the field.
func (col *Column) SetString(field String, value string) { col.set(field, value) }

// SetTime maps the time.Time value to the field.
func (col *Column) SetTime(field Time, value time.Time) { col.set(field, value) }

// SetArray maps the array value to the field. The value should be []string,
// []int, []int64, []int32, []int16, []float64, []float32 or []bool.
func (col *Column) SetArray(field Array, value any) { col.set(field, ArrayValue(value)) }

// SetEnum maps the enum value to the field.
func (col *Column) SetEnum(field Enum, value Enumeration) { col.set(field, EnumValue(value)) }

// SetJSON maps the JSON value to the field. The value should be able to be
// convertible to JSON using json.Marshal.
func (col *Column) SetJSON(field JSON, value any) { col.set(field, JSONValue(value)) }

// SetUUID maps the UUID value to the field. The value's type or underlying
// type should be [16]byte.
func (col *Column) SetUUID(field UUID, value any) { col.set(field, UUIDValue(value)) }
