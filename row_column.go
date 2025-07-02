package sq

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/blink-io/sq/internal/googleuuid"
	"github.com/blink-io/sq/internal/pqarray"

	"github.com/spf13/cast"
)

// RowMapper defines row mapper function.
type (
	RowMapper[T any] func(context.Context, *Row) T
)

type (
	NullBool    = sql.Null[bool]
	NullInt     = sql.Null[int]
	NullInt8    = sql.Null[int8]
	NullInt16   = sql.Null[int16]
	NullInt32   = sql.Null[int32]
	NullInt64   = sql.Null[int64]
	NullUint    = sql.Null[uint]
	NullUint8   = sql.Null[uint8]
	NullUint16  = sql.Null[uint16]
	NullUint32  = sql.Null[uint32]
	NullUint64  = sql.Null[uint64]
	NullFloat32 = sql.Null[float32]
	NullFloat64 = sql.Null[float64]
	NullString  = sql.Null[string]
	NullTime    = sql.Null[time.Time]

	NullNumberType = interface {
		NullInt | NullInt8 | NullInt16 | NullInt32 | NullInt64
		NullUint | NullUint16 | NullUint32 | NullUint64 | NullInt64
		NullFloat32 | NullFloat64
	}

	NumericType = interface {
		~int | ~int8 | ~int16 | ~int32 | ~int64 |
			~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
			~float32 | ~float64
	}
)

// Row represents the state of a row after a call to rows.Next().
type Row struct {
	dialect       string
	sqlRows       *sql.Rows
	runningIndex  int
	fields        []Field
	scanDest      []any
	queryIsStatic bool
	columns       []string
	columnTypes   []*sql.ColumnType
	values        []any
	columnIndex   map[string]int
}

// Columns return the names of the columns returned by the query. This method
// can only be called in a rowMapper if it is paired with a raw SQL query e.g.
// Queryf("SELECT * FROM my_table"). Otherwise, an error will be returned.
func (row *Row) Columns() []string {
	if row.queryIsStatic {
		return row.columns
	}
	if row.sqlRows == nil {
		return nil
	}
	columns, err := row.sqlRows.Columns()
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"sqlRows.Columns: %w", err))
	}
	return columns
}

// ColumnTypes returns the column types returned by the query. This method can
// only be called in a rowMapper if it is paired with a raw SQL query e.g.
// Queryf("SELECT * FROM my_table"). Otherwise, an error will be returned.
func (row *Row) ColumnTypes() []*sql.ColumnType {
	if row.queryIsStatic {
		return row.columnTypes
	}
	if row.sqlRows == nil {
		return nil
	}
	columnTypes, err := row.sqlRows.ColumnTypes()
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"sqlRows.ColumnTypes: %w", err))
	}
	return columnTypes
}

// Values returns the values of the current row. This method can only be called
// in a rowMapper if it is paired with a raw SQL query e.g. Queryf("SELECT *
// FROM my_table"). Otherwise, an error will be returned.
func (row *Row) Values() []any {
	if row.queryIsStatic {
		values := make([]any, len(row.values))
		copy(values, row.values)
		return values
	}
	if row.sqlRows == nil {
		return nil
	}
	columns, err := row.sqlRows.Columns()
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"sqlRows.Columns: %w", err))
	}
	values := make([]any, len(columns))
	scanDest := make([]any, len(columns))
	for i := range values {
		scanDest[i] = &values[i]
	}
	err = row.sqlRows.Scan(scanDest...)
	if err != nil {
		panic(fmt.Errorf(callsite(1)+"sqlRows.Scan: %w", err))
	}
	return values
}

// Value returns the value of the expression. It is intended for use cases
// where you only know the name of the column but not its type to scan into.
// The underlying type of the value is determined by the database driver you
// are using.
func (row *Row) Value(format string, values ...any) any {
	if row.queryIsStatic {
		index, ok := row.columnIndex[format]
		if !ok {
			panic(fmt.Errorf(callsite(1)+"column %s is not present in query (available columns: %s)", format, strings.Join(row.columns, ", ")))
		}
		return row.values[index]
	}
	if row.sqlRows == nil {
		var value any
		row.fields = append(row.fields, Expr(format, values...))
		row.scanDest = append(row.scanDest, &value)
		return nil
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*any)
	return *scanDest
}

// Scan scans the expression into destPtr.
func (row *Row) Scan(destPtr any, format string, values ...any) {
	markQueryIsStaticPanic(row, "Scan")
	row.scan(destPtr, Expr(format, values...), 1)
}

// ScanField scans the field into destPtr.
func (row *Row) ScanField(destPtr any, field Field) {
	markQueryIsStaticPanic(row, "ScanField")
	row.scan(destPtr, field, 1)
}

func (row *Row) scan(destPtr any, field Field, skip int) {
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		switch destPtr.(type) {
		case *bool, *sql.NullBool, *NullBool:
			row.scanDest = append(row.scanDest, &NullBool{})
		case *float32, NullFloat32:
			row.scanDest = append(row.scanDest, &NullFloat32{})
		case *float64, *sql.NullFloat64, NullFloat64:
			row.scanDest = append(row.scanDest, &NullFloat64{})
		case *int, *NullInt:
			row.scanDest = append(row.scanDest, &NullInt{})
		case *int8, *NullInt8:
			row.scanDest = append(row.scanDest, &NullInt8{})
		case *int16, *NullInt16:
			row.scanDest = append(row.scanDest, &NullInt16{})
		case *int32, *sql.NullInt32, NullInt32:
			row.scanDest = append(row.scanDest, &NullInt32{})
		case *int64, *sql.NullInt64, NullInt64:
			row.scanDest = append(row.scanDest, &NullInt64{})
		case *uint, *NullUint:
			row.scanDest = append(row.scanDest, &NullUint{})
		case *uint8, *NullUint8:
			row.scanDest = append(row.scanDest, &NullInt8{})
		case *uint16, *NullUint16:
			row.scanDest = append(row.scanDest, &NullUint16{})
		case *uint32, NullUint32:
			row.scanDest = append(row.scanDest, &NullUint32{})
		case *uint64, NullUint64:
			row.scanDest = append(row.scanDest, &NullUint64{})
		case *string, *sql.NullString, NullString:
			row.scanDest = append(row.scanDest, &NullString{})
		case *time.Time, *sql.NullTime, NullTime:
			row.scanDest = append(row.scanDest, &NullTime{})
		default:
			if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
				panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
			}
			row.scanDest = append(row.scanDest, destPtr)
		}
		return
	}
	defer func() {
		row.runningIndex++
	}()
	switch destPtr := destPtr.(type) {
	case *bool:
		scanDest := row.scanDest[row.runningIndex].(*NullBool)
		*destPtr = scanDest.V
	case *sql.NullBool:
		scanDest := row.scanDest[row.runningIndex].(*sql.NullBool)
		*destPtr = *scanDest
	case *NullBool:
		scanDest := row.scanDest[row.runningIndex].(*NullBool)
		*destPtr = *scanDest

	case *float32:
		scanDest := row.scanDest[row.runningIndex].(*NullFloat32)
		*destPtr = scanDest.V
	case *NullFloat32:
		scanDest := row.scanDest[row.runningIndex].(*NullFloat32)
		*destPtr = *scanDest

	case *float64:
		scanDest := row.scanDest[row.runningIndex].(*NullFloat64)
		*destPtr = scanDest.V
	case *sql.NullFloat64:
		scanDest := row.scanDest[row.runningIndex].(*sql.NullFloat64)
		*destPtr = *scanDest
	case *NullFloat64:
		scanDest := row.scanDest[row.runningIndex].(*NullFloat64)
		*destPtr = *scanDest

	case *int:
		scanDest := row.scanDest[row.runningIndex].(*NullInt)
		*destPtr = scanDest.V
	case *NullInt:
		scanDest := row.scanDest[row.runningIndex].(*NullInt)
		*destPtr = *scanDest

	case *int8:
		scanDest := row.scanDest[row.runningIndex].(*NullInt8)
		*destPtr = scanDest.V
	case *NullInt8:
		scanDest := row.scanDest[row.runningIndex].(*NullInt8)
		*destPtr = *scanDest

	case *int16:
		scanDest := row.scanDest[row.runningIndex].(*NullInt16)
		*destPtr = scanDest.V
	case *NullInt16:
		scanDest := row.scanDest[row.runningIndex].(*NullInt16)
		*destPtr = *scanDest

	case *int32:
		scanDest := row.scanDest[row.runningIndex].(*NullInt32)
		*destPtr = scanDest.V
	case *sql.NullInt32:
		scanDest := row.scanDest[row.runningIndex].(*sql.NullInt32)
		*destPtr = *scanDest
	case *NullInt32:
		scanDest := row.scanDest[row.runningIndex].(*NullInt32)
		*destPtr = *scanDest

	case *int64:
		scanDest := row.scanDest[row.runningIndex].(*NullInt64)
		*destPtr = scanDest.V
	case *sql.NullInt64:
		scanDest := row.scanDest[row.runningIndex].(*sql.NullInt64)
		*destPtr = *scanDest
	case *NullInt64:
		scanDest := row.scanDest[row.runningIndex].(*NullInt64)
		*destPtr = *scanDest

	case *uint:
		scanDest := row.scanDest[row.runningIndex].(*NullUint)
		*destPtr = scanDest.V
	case *NullUint:
		scanDest := row.scanDest[row.runningIndex].(*NullUint)
		*destPtr = *scanDest

	case *uint8:
		scanDest := row.scanDest[row.runningIndex].(*NullUint8)
		*destPtr = scanDest.V
	case *NullUint8:
		scanDest := row.scanDest[row.runningIndex].(*NullUint8)
		*destPtr = *scanDest

	case *uint16:
		scanDest := row.scanDest[row.runningIndex].(*NullUint16)
		*destPtr = scanDest.V
	case *NullUint16:
		scanDest := row.scanDest[row.runningIndex].(*NullUint16)
		*destPtr = *scanDest

	case *uint32:
		scanDest := row.scanDest[row.runningIndex].(*NullUint32)
		*destPtr = scanDest.V
	case *NullUint32:
		scanDest := row.scanDest[row.runningIndex].(*NullUint32)
		*destPtr = *scanDest

	case *uint64:
		scanDest := row.scanDest[row.runningIndex].(*NullUint64)
		*destPtr = scanDest.V
	case *NullUint64:
		scanDest := row.scanDest[row.runningIndex].(*NullUint64)
		*destPtr = *scanDest

	case *string:
		scanDest := row.scanDest[row.runningIndex].(*NullString)
		*destPtr = scanDest.V
	case *sql.NullString:
		scanDest := row.scanDest[row.runningIndex].(*sql.NullString)
		*destPtr = *scanDest
	case *NullString:
		scanDest := row.scanDest[row.runningIndex].(*NullString)
		*destPtr = *scanDest

	case *time.Time:
		scanDest := row.scanDest[row.runningIndex].(*NullTime)
		*destPtr = scanDest.V
	case *sql.NullTime:
		scanDest := row.scanDest[row.runningIndex].(*sql.NullTime)
		*destPtr = *scanDest
	case *NullTime:
		scanDest := row.scanDest[row.runningIndex].(*NullTime)
		*destPtr = *scanDest
	default:
		destValue := reflect.ValueOf(destPtr).Elem()
		srcValue := reflect.ValueOf(row.scanDest[row.runningIndex]).Elem()
		destValue.Set(srcValue)
	}
}

// Array scans the array expression into destPtr. The destPtr must be a pointer
// to a []string, []int, []int64, []int32, []float64, []float32 or []bool.
func (row *Row) Array(destPtr any, format string, values ...any) {
	//markQueryIsStaticPanic(row, "Array")
	row.array(destPtr, Expr(format, values...), 1)
}

// ArrayField scans the array field into destPtr. The destPtr must be a pointer
// to a []string, []int, []int64, []int32, []float64, []float32 or []bool.
func (row *Row) ArrayField(destPtr any, field Array) {
	markQueryIsStaticPanic(row, "ArrayField")
	row.array(destPtr, field, 1)
}

func (row *Row) array(destPtr any, field Array, skip int) {
	if row.sqlRows == nil {
		if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		if row.dialect == DialectPostgres {
			switch destPtr.(type) {
			case *[]string, *[]int, *[]int64, *[]int32, *[]int16, *[]float64, *[]float32, *[]bool:
				break
			default:
				panic(fmt.Errorf(callsite(skip+1)+"destptr (%T) must be either a pointer to a []string, []int, []int64, "+
					"[]int32, []int16, []float64, []float32 or []bool", destPtr))
			}
		}
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect:     row.dialect,
			displayType: displayTypeString,
		})
		return
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*nullBytes)
	if !scanDest.valid {
		return
	}
	if row.dialect != DialectPostgres {
		err := json.Unmarshal(scanDest.bytes, destPtr)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unmarshaling json %q into %T: %w", string(scanDest.bytes), destPtr, err))
		}
		return
	}
	switch destPtr := destPtr.(type) {
	case *[]string:
		var array pqarray.StringArray
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to string array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]int:
		var array pqarray.Int64Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int64 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = (*destPtr)[:cap(*destPtr)]
		if len(*destPtr) < len(array) {
			*destPtr = make([]int, len(array))
		}
		*destPtr = (*destPtr)[:len(array)]
		for i, num := range array {
			(*destPtr)[i] = int(num)
		}
	case *[]int64:
		var array pqarray.Int64Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int64 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]int32:
		var array pqarray.Int32Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int32 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]int16:
		var array pqarray.Int16Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to int16 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]float64:
		var array pqarray.Float64Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to float64 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]float32:
		var array pqarray.Float32Array
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to float32 array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	case *[]bool:
		var array pqarray.BoolArray
		err := array.Scan(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"unable to convert %q to bool array: %w", string(scanDest.bytes), err))
		}
		*destPtr = array
	default:
		panic(fmt.Errorf(callsite(skip+1)+"destptr (%T) must be either a pointer to a []string, []int, []int64, []int32, []float64, []float32 or []bool", destPtr))
	}
}

// Bytes return the []byte value of the expression.
func (row *Row) Bytes(format string, values ...any) []byte {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value := value.(type) {
		case []byte:
			return value
		case string:
			return []byte(value)
		case nil:
			return nil
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not []byte", value))
		}
	}
	if row.sqlRows == nil {
		row.fields = append(row.fields, Expr(format, values...))
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect: row.dialect,
		})
		return nil
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*nullBytes)
	var b []byte
	if scanDest.valid {
		b = make([]byte, len(scanDest.bytes))
		copy(b, scanDest.bytes)
	}
	return b
}

// BytesField returns the []byte value of the field.
func (row *Row) BytesField(field Binary) []byte {
	markQueryIsStaticPanic(row, "BytesField")
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect: row.dialect,
		})
		return nil
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*nullBytes)
	var b []byte
	if scanDest.valid {
		b = make([]byte, len(scanDest.bytes))
		copy(b, scanDest.bytes)
	}
	return b
}

// == Bool == //

// Bool returns the bool value of the expression.
func (row *Row) Bool(format string, values ...any) bool {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value := value.(type) {
		case int64:
			if value == 1 {
				return true
			}
			if value == 0 {
				return false
			}
			panic(fmt.Errorf(callsite(1)+"%d is int64, not bool", value))
		case bool:
			return value
		case []byte:
			// Special case: go-mysql-driver returns everything as []byte.
			if string(value) == "1" {
				return true
			}
			if string(value) == "0" {
				return false
			}
			panic(fmt.Errorf(callsite(1)+"%#v is []byte, not bool", value))
		case nil:
			return false
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not bool", value))
		}
	}
	return row.NullBoolField(Expr(format, values...)).V
}

// BoolField returns the bool value of the field.
func (row *Row) BoolField(field Boolean) bool {
	markQueryIsStaticPanic(row, "BoolField")
	return row.NullBoolField(field).V
}

// NullBool returns the NullBool value of the expression.
func (row *Row) NullBool(format string, values ...any) NullBool {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value := value.(type) {
		case int64:
			if value == 1 {
				return NullBool{V: true, Valid: true}
			}
			if value == 0 {
				return NullBool{V: false, Valid: true}
			}
			panic(fmt.Errorf(callsite(1)+"%d is int64, not bool", value))
		case bool:
			return NullBool{V: value, Valid: true}
		case []byte:
			// Special case: go-mysql-driver returns everything as []byte.
			if string(value) == "1" {
				return NullBool{V: true, Valid: true}
			}
			if string(value) == "0" {
				return NullBool{V: false, Valid: true}
			}
			panic(fmt.Errorf(callsite(1)+"%d is []byte, not bool", value))
		case nil:
			return NullBool{}
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not bool", value))
		}
	}
	return row.NullBoolField(Expr(format, values...))
}

// NullBoolField returns the NullBool value of the field.
func (row *Row) NullBoolField(field Boolean) NullBool {
	markQueryIsStaticPanic(row, "NullBoolField")
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &NullBool{})
		return NullBool{}
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*NullBool)
	return *scanDest
}

// Enum scans the enum expression into destPtr.
func (row *Row) Enum(destPtr Enumeration, format string, values ...any) {
	//markQueryIsStaticPanic(row, "Enum")
	row.enum(destPtr, Expr(format, values...), 1)
}

// EnumField scans the enum field into destPtr.
func (row *Row) EnumField(destPtr Enumeration, field Enum) {
	markQueryIsStaticPanic(row, "EnumField")
	row.enum(destPtr, field, 1)
}

func (row *Row) enum(destPtr Enumeration, field Enum, skip int) {
	if row.sqlRows == nil {
		destType := reflect.TypeOf(destPtr)
		if destType.Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		row.fields = append(row.fields, field)
		switch destType.Elem().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.String:
			row.scanDest = append(row.scanDest, &NullString{})
		default:
			panic(fmt.Errorf(callsite(skip+1)+"underlying type of %[1]v is neither an integer or string (%[1]T)", destPtr))
		}
		return
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*NullString)
	names := destPtr.Enumerate()
	enumIndex := 0
	destValue := reflect.ValueOf(destPtr).Elem()
	if scanDest.Valid {
		enumIndex = getEnumIndex(scanDest.V, names, destValue.Type())
	}
	if enumIndex < 0 {
		panic(fmt.Errorf(callsite(skip+1)+"%q is not a valid %T", scanDest.V, destPtr))
	}
	switch destValue.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		destValue.SetInt(int64(enumIndex))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		destValue.SetUint(uint64(enumIndex))
	case reflect.String:
		destValue.SetString(scanDest.V)
	default:
	}
}

func castNumericType[T NumericType](value any, valueKind reflect.Kind, convFunc func(i any) (T, error)) T {
	n, err := convFunc(value)
	if err != nil {
		tstr := valueKind.String()
		panic(fmt.Errorf(callsite(1)+"%d is []byte, not %s", value, tstr))
	}
	return n
}

func isIntegerType(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int,
		reflect.Int8,
		reflect.Int16,
		reflect.Int32,
		reflect.Int64,
		reflect.Uint,
		reflect.Uint8,
		reflect.Uint16,
		reflect.Uint32,
		reflect.Uint64:
		return true
	default:
		return false
	}
}

func isFloatType(kind reflect.Kind) bool {
	switch kind {
	case reflect.Float32,
		reflect.Float64:
		return true
	default:
		return false
	}
}

func handleNumericValue[T NumericType](value any) T {
	tt := GetType[T]()
	switch value.(type) {
	case float32,
		float64,
		int,
		int8,
		int16,
		int32,
		int64,
		uint,
		uint8,
		uint16,
		uint32,
		uint64,
		[]byte:
		var vv T
		vvKind := reflect.TypeOf(vv).Kind()
		// Special case: go-mysql-driver returns everything as []byte.
		if bytes, ok := value.([]byte); ok {
			var errv error
			if isIntegerType(vvKind) {
				value, errv = strconv.ParseInt(string(bytes), 10, 64)
			} else if isFloatType(vvKind) {
				value, errv = strconv.ParseFloat(string(bytes), 64)
			}
			if errv != nil {
				panic(fmt.Errorf(callsite(1)+"%d is []byte, and cannot convert to %s", value, vvKind.String()))
			}
		}
		switch vvKind {
		case reflect.Int:
			return T(castNumericType[int](value, reflect.Int, cast.ToIntE))
		case reflect.Int8:
			return T(castNumericType[int8](value, reflect.Int8, cast.ToInt8E))
		case reflect.Int16:
			return T(castNumericType[int16](value, reflect.Int16, cast.ToInt16E))
		case reflect.Int32:
			return T(castNumericType[int32](value, reflect.Int32, cast.ToInt32E))
		case reflect.Int64:
			return T(castNumericType[int64](value, reflect.Int64, cast.ToInt64E))
		case reflect.Uint:
			return T(castNumericType[uint](value, reflect.Uint, cast.ToUintE))
		case reflect.Uint8:
			return T(castNumericType[uint8](value, reflect.Uint8, cast.ToUint8E))
		case reflect.Uint16:
			return T(castNumericType[uint16](value, reflect.Uint16, cast.ToUint16E))
		case reflect.Uint32:
			return T(castNumericType[uint32](value, reflect.Uint32, cast.ToUint32E))
		case reflect.Uint64:
			return T(castNumericType[uint64](value, reflect.Uint64, cast.ToUint64E))
		case reflect.Float32:
			return T(castNumericType[float32](value, reflect.Float32, cast.ToFloat32E))
		case reflect.Float64:
			return T(castNumericType[float64](value, reflect.Float64, cast.ToFloat64E))
		default:
			panic(fmt.Errorf(callsite(1)+"%d is []byte and target type %s is unsupported", value, tt))
		}
	case bool:
		panic(fmt.Errorf(callsite(1)+"%v is bool, not "+tt, value))
	case string:
		panic(fmt.Errorf(callsite(1)+"%q is string, not float64", value))
	case time.Time:
		panic(fmt.Errorf(callsite(1)+"%v is time.Time, not float64", value))
	case nil:
		return 0
	default:
		panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not "+tt, value))
	}
}

func NumericValue[T NumericType](row *Row, format string, values ...any) T {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		return handleNumericValue[T](row.values[index])
	}
	return NullNumericField[T](row, Expr(format, values...)).V
}

func NullNumericField[T NumericType](row *Row, field Number) sql.Null[T] {
	//markQueryIsStaticPanic(row, GetType[T]())
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &sql.Null[T]{})
		return sql.Null[T]{}
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*sql.Null[T])
	return *scanDest
}

// NullNumericValue returns the null number value of the expression.
func NullNumericValue[T NumericType](row *Row, format string, values ...any) sql.Null[T] {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value.(type) {
		case nil:
			return sql.Null[T]{}
		default:
			v := NumericValue[T](row, format, values...)
			return sql.Null[T]{V: v, Valid: true}
		}
	}
	return NullNumericField[T](row, Expr(format, values...))
}

// Float32 returns the float32 value of the expression.
func (row *Row) Float32(format string, values ...any) float32 {
	return NumericValue[float32](row, format, values...)
}

// Float32Field returns the float32 value of the field.
func (row *Row) Float32Field(field Number) float32 {
	markQueryIsStaticPanic(row, "Float32Field")
	return row.NullFloat32Field(field).V
}

// NullFloat32 returns the NullFloat32 value of the expression.
func (row *Row) NullFloat32(format string, values ...any) NullFloat32 {
	return NullNumericValue[float32](row, format, values...)
}

// NullFloat32Field returns the NullFloat32 value of the field.
func (row *Row) NullFloat32Field(field Number) NullFloat32 {
	markQueryIsStaticPanic(row, "NullFloat32Field")
	return NullNumericField[float32](row, field)
}

// Float64 returns the float64 value of the expression.
func (row *Row) Float64(format string, values ...any) float64 {
	return NumericValue[float64](row, format, values...)
}

// Float64Field returns the float64 value of the field.
func (row *Row) Float64Field(field Number) float64 {
	markQueryIsStaticPanic(row, "Float64Field")
	return row.NullFloat64Field(field).V
}

// NullFloat64 returns the NullFloat64 value of the expression.
func (row *Row) NullFloat64(format string, values ...any) NullFloat64 {
	return NullNumericValue[float64](row, format, values...)
}

// NullFloat64Field returns the NullFloat64 value of the field.
func (row *Row) NullFloat64Field(field Number) NullFloat64 {
	markQueryIsStaticPanic(row, "NullFloat64Field")
	return NullNumericField[float64](row, field)
}

// Int returns the int value of the expression.
func (row *Row) Int(format string, values ...any) int {
	return NumericValue[int](row, format, values...)
}

// IntField returns the int value of the field.
func (row *Row) IntField(field Number) int {
	markQueryIsStaticPanic(row, "IntField")
	return int(row.NullInt64Field(field).V)
}

// NullInt returns the NullInt value of the expression.
func (row *Row) NullInt(format string, values ...any) NullInt {
	return NullNumericValue[int](row, format, values...)
}

// NullIntField returns the int value of the field.
func (row *Row) NullIntField(field Number) NullInt {
	markQueryIsStaticPanic(row, "NullIntField")
	return NullNumericField[int](row, field)
}

// Int8 returns the int value of the expression.
func (row *Row) Int8(format string, values ...any) int8 {
	return NumericValue[int8](row, format, values...)
}

// Int8Field returns the int value of the field.
func (row *Row) Int8Field(field Number) int8 {
	markQueryIsStaticPanic(row, "Int8Field")
	return NullNumericField[int8](row, field).V
}

// NullInt8 returns the NullInt8 value of the expression.
func (row *Row) NullInt8(format string, values ...any) NullInt8 {
	return NullNumericValue[int8](row, format, values...)
}

// NullInt8Field returns the int8 value of the field.
func (row *Row) NullInt8Field(field Number) NullInt8 {
	markQueryIsStaticPanic(row, "NullInt8Field")
	return NullNumericField[int8](row, field)
}

// Int16 returns the int value of the expression.
func (row *Row) Int16(format string, values ...any) int16 {
	return NumericValue[int16](row, format, values...)
}

// Int16Field returns the int value of the field.
func (row *Row) Int16Field(field Number) int16 {
	markQueryIsStaticPanic(row, "Int16Field")
	return NullNumericField[int16](row, field).V
}

// NullInt16 returns the NullInt16 value of the expression.
func (row *Row) NullInt16(format string, values ...any) NullInt16 {
	return NullNumericValue[int16](row, format, values...)
}

// NullInt16Field returns the NullInt16 value of the field.
func (row *Row) NullInt16Field(field Number) NullInt16 {
	markQueryIsStaticPanic(row, "NullInt16Field")
	return NullNumericField[int16](row, field)
}

// Int32 returns the int value of the expression.
func (row *Row) Int32(format string, values ...any) int32 {
	return NumericValue[int32](row, format, values...)
}

// Int32Field returns the int value of the field.
func (row *Row) Int32Field(field Number) int32 {
	markQueryIsStaticPanic(row, "Int32Field")
	return NullNumericField[int32](row, field).V
}

// NullInt32 returns the NullInt32 value of the expression.
func (row *Row) NullInt32(format string, values ...any) NullInt32 {
	return NullNumericValue[int32](row, format, values...)
}

// NullInt32Field returns the NullInt32 value of the field.
func (row *Row) NullInt32Field(field Number) NullInt32 {
	markQueryIsStaticPanic(row, "NullInt32Field")
	return NullNumericField[int32](row, field)
}

// Int64 returns the int64 value of the expression.
func (row *Row) Int64(format string, values ...any) int64 {
	return NumericValue[int64](row, format, values...)
}

// Int64Field returns the int64 value of the field.
func (row *Row) Int64Field(field Number) int64 {
	markQueryIsStaticPanic(row, "Int64Field")
	return row.NullInt64Field(field).V
}

// NullInt64 returns the NullInt64 value of the expression.
func (row *Row) NullInt64(format string, values ...any) NullInt64 {
	return NullNumericValue[int64](row, format, values...)
}

// NullInt64Field returns the NullInt64 value of the field.
func (row *Row) NullInt64Field(field Number) NullInt64 {
	markQueryIsStaticPanic(row, "NullInt64Field")
	return NullNumericField[int64](row, field)
}

// Uint returns the uint value of the expression.
func (row *Row) Uint(format string, values ...any) uint {
	return NumericValue[uint](row, format, values...)
}

// UintField returns the uint value of the field.
func (row *Row) UintField(field Number) int {
	markQueryIsStaticPanic(row, "UintField")
	return row.NullIntField(field).V
}

// NullUint returns the NullUint value of the expression.
func (row *Row) NullUint(format string, values ...any) NullUint {
	return NullNumericValue[uint](row, format, values...)
}

// NullUintField returns the uint value of the field.
func (row *Row) NullUintField(field Number) NullUint {
	markQueryIsStaticPanic(row, "NullUintField")
	return NullNumericField[uint](row, field)
}

// Uint8 returns the uint8 value of the expression.
func (row *Row) Uint8(format string, values ...any) uint8 {
	return NumericValue[uint8](row, format, values...)
}

// Uint8Field returns the uint8 value of the field.
func (row *Row) Uint8Field(field Number) int8 {
	markQueryIsStaticPanic(row, "Uint8Field")
	return row.NullInt8Field(field).V
}

// NullUint8 returns the NullUint8 value of the expression.
func (row *Row) NullUint8(format string, values ...any) NullUint8 {
	return NullNumericValue[uint8](row, format, values...)
}

// NullUint8Field returns the NullUint8 value of the field.
func (row *Row) NullUint8Field(field Number) NullUint8 {
	markQueryIsStaticPanic(row, "NullUint8Field")
	return NullNumericField[uint8](row, field)
}

// Uint16 returns the uint16 value of the expression.
func (row *Row) Uint16(format string, values ...any) uint16 {
	return NumericValue[uint16](row, format, values...)
}

// Uint16Field returns the uint16 value of the field.
func (row *Row) Uint16Field(field Number) uint16 {
	markQueryIsStaticPanic(row, "Uint16Field")
	return row.NullUint16Field(field).V
}

// NullUint16 returns the NullUint16 value of the expression.
func (row *Row) NullUint16(format string, values ...any) NullUint16 {
	return NullNumericValue[uint16](row, format, values...)
}

// NullUint16Field returns the NullUint16 value of the field.
func (row *Row) NullUint16Field(field Number) NullUint16 {
	markQueryIsStaticPanic(row, "NullUint16Field")
	return NullNumericField[uint16](row, field)
}

// Uint32 returns the uint32 value of the expression.
func (row *Row) Uint32(format string, values ...any) uint32 {
	return NumericValue[uint32](row, format, values...)
}

// Uint32Field returns the uint32 value of the field.
func (row *Row) Uint32Field(field Number) uint32 {
	markQueryIsStaticPanic(row, "Uint32Field")
	return row.NullUint32Field(field).V
}

// NullUint32 returns the NullUint32 value of the expression.
func (row *Row) NullUint32(format string, values ...any) NullUint32 {
	return NullNumericValue[uint32](row, format, values...)
}

// NullUint32Field returns the NullUint32 value of the field.
func (row *Row) NullUint32Field(field Number) NullUint32 {
	markQueryIsStaticPanic(row, "NullUint32Field")
	return NullNumericField[uint32](row, field)
}

// Uint64 returns the uint64 value of the expression.
func (row *Row) Uint64(format string, values ...any) uint64 {
	return NumericValue[uint64](row, format, values...)
}

// Uint64Field returns the uint64 value of the field.
func (row *Row) Uint64Field(field Number) uint64 {
	markQueryIsStaticPanic(row, "Uint64Field")
	return row.NullUint64Field(field).V
}

// NullUint64 returns the NullUint64 value of the expression.
func (row *Row) NullUint64(format string, values ...any) NullUint64 {
	return NullNumericValue[uint64](row, format, values...)
}

// NullUint64Field returns the NullUint64 value of the field.
func (row *Row) NullUint64Field(field Number) NullUint64 {
	markQueryIsStaticPanic(row, "NullUint64Field")
	return NullNumericField[uint64](row, field)
}

// JSON scans the JSON expression into destPtr.
func (row *Row) JSON(destPtr any, format string, values ...any) {
	markQueryIsStaticPanic(row, "JSON")
	row.json(destPtr, Expr(format, values...), 1)
}

// JSONField scans the JSON field into destPtr.
func (row *Row) JSONField(destPtr any, field JSON) {
	markQueryIsStaticPanic(row, "JSONField")
	row.json(destPtr, field, 1)
}

func (row *Row) json(destPtr any, field JSON, skip int) {
	if row.sqlRows == nil {
		if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
			panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
		}
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect:     row.dialect,
			displayType: displayTypeString,
		})
		return
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*nullBytes)
	if scanDest.valid {
		err := json.Unmarshal(scanDest.bytes, destPtr)
		if err != nil {
			_, file, line, _ := runtime.Caller(skip + 1)
			panic(fmt.Errorf(callsite(skip+1)+"unmarshaling json %q into %T: %w", file, line, string(scanDest.bytes), destPtr, err))
		}
	}
}

// String returns the string value of the expression.
func (row *Row) String(format string, values ...any) string {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value := value.(type) {
		case int64:
			panic(fmt.Errorf(callsite(1)+"%d is int64, not string", value))
		case float64:
			panic(fmt.Errorf(callsite(1)+"%d is float64, not string", value))
		case bool:
			panic(fmt.Errorf(callsite(1)+"%v is bool, not string", value))
		case []byte:
			return string(value)
		case string:
			return value
		case time.Time:
			panic(fmt.Errorf(callsite(1)+"%v is time.Time, not string", value))
		case nil:
			return ""
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not string", value))
		}
	}
	return row.NullStringField(Expr(format, values...)).V
}

// StringField returns the string value of the field.
func (row *Row) StringField(field String) string {
	markQueryIsStaticPanic(row, "StringField")
	return row.NullStringField(field).V
}

// NullString returns the NullString value of the expression.
func (row *Row) NullString(format string, values ...any) NullString {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value := value.(type) {
		case []byte:
			return NullString{V: string(value), Valid: true}
		case string:
			return NullString{V: value, Valid: true}
		case nil:
			return NullString{}
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not string", value))
		}
	}
	return row.NullStringField(Expr(format, values...))
}

// NullStringField returns the NullString value of the field.
func (row *Row) NullStringField(field String) NullString {
	markQueryIsStaticPanic(row, "NullStringField")
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &NullString{})
		return NullString{}
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*NullString)
	return *scanDest
}

// https://github.com/mattn/go-sqlite3/blob/4396a38886da660e403409e35ef4a37906bf0975/sqlite3.go#L209
var sqliteTimestampFormats = []string{
	"2006-01-02 15:04:05.999999999-07:00",
	"2006-01-02T15:04:05.999999999-07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
	"2006-01-02",
}

// Time returns the time.Time value of the expression.
func (row *Row) Time(format string, values ...any) time.Time {
	if row.queryIsStatic {
		index := markNoColumnIndexPanic(row, format)
		value := row.values[index]
		switch value := value.(type) {
		case []byte:
			// Special case: go-mysql-driver returns everything as []byte.
			s := strings.TrimSuffix(string(value), "Z")
			for _, format := range sqliteTimestampFormats {
				if t, err := time.ParseInLocation(format, s, time.UTC); err == nil {
					return t
				}
			}
			panic(fmt.Errorf(callsite(1)+"%d is []byte, not time.Time", value))
		case time.Time:
			return value
		case nil:
			return time.Time{}
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not time.Time", value))
		}
	}
	return row.NullTimeField(Expr(format, values...)).V
}

// TimeField returns the time.Time value of the field.
func (row *Row) TimeField(field Time) time.Time {
	markQueryIsStaticPanic(row, "TimeField")
	return row.NullTimeField(field).V
}

// NullTime returns the NullTime value of the expression.
func (row *Row) NullTime(format string, values ...any) NullTime {
	if row.queryIsStatic {
		index, ok := row.columnIndex[format]
		if !ok {
			panic(fmt.Errorf(callsite(1)+"column %s does not exist (available columns: %s)", format, strings.Join(row.columns, ", ")))
		}
		value := row.values[index]
		switch value := value.(type) {
		case []byte:
			// Special case: go-mysql-driver returns everything as []byte.
			s := strings.TrimSuffix(string(value), "Z")
			for _, format := range sqliteTimestampFormats {
				if t, err := time.ParseInLocation(format, s, time.UTC); err == nil {
					return NullTime{V: t, Valid: true}
				}
			}
			panic(fmt.Errorf(callsite(1)+"%d is []byte, not time.Time", value))
		case string:
			panic(fmt.Errorf(callsite(1)+"%q is string, not time.Time", value))
		case time.Time:
			return NullTime{V: value, Valid: true}
		case nil:
			return NullTime{}
		default:
			panic(fmt.Errorf(callsite(1)+"%[1]v is %[1]T, not time.Time", value))
		}
	}
	return row.NullTimeField(Expr(format, values...))
}

// NullTimeField returns the NullTime value of the field.
func (row *Row) NullTimeField(field Time) NullTime {
	markQueryIsStaticPanic(row, "NullTimeField")
	if row.sqlRows == nil {
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &NullTime{})
		return NullTime{}
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*NullTime)
	return *scanDest
}

// UUID scans the UUID expression into destPtr.
func (row *Row) UUID(destPtr any, format string, values ...any) {
	// TODO If we check queryIsStatic, we cannot fetch UUID column alone.
	row.uuid(destPtr, Expr(format, values...), 1)
}

// UUIDField scans the UUID field into destPtr.
func (row *Row) UUIDField(destPtr any, field UUID) {
	markQueryIsStaticPanic(row, "UUIDField")
	row.uuid(destPtr, field, 1)
}

func (row *Row) uuid(destPtr any, field UUID, skip int) {
	if row.sqlRows == nil {
		if _, ok := destPtr.(*[16]byte); !ok {
			if reflect.TypeOf(destPtr).Kind() != reflect.Ptr {
				panic(fmt.Errorf(callsite(skip+1)+"cannot pass in non pointer value (%#v) as destPtr", destPtr))
			}
			destValue := reflect.ValueOf(destPtr).Elem()
			if destValue.Kind() != reflect.Array || destValue.Len() != 16 || destValue.Type().Elem().Kind() != reflect.Uint8 {
				panic(fmt.Errorf(callsite(skip+1)+"%T is not a pointer to a [16]byte", destPtr))
			}
		}
		row.fields = append(row.fields, field)
		row.scanDest = append(row.scanDest, &nullBytes{
			dialect:     row.dialect,
			displayType: displayTypeUUID,
		})
		return
	}
	defer func() {
		row.runningIndex++
	}()
	scanDest := row.scanDest[row.runningIndex].(*nullBytes)
	var err error
	var uuid [16]byte
	if len(scanDest.bytes) == 16 {
		copy(uuid[:], scanDest.bytes)
	} else if len(scanDest.bytes) > 0 {
		uuid, err = googleuuid.ParseBytes(scanDest.bytes)
		if err != nil {
			panic(fmt.Errorf(callsite(skip+1)+"parsing %q as UUID string: %w", string(scanDest.bytes), err))
		}
	}
	if destArrayPtr, ok := destPtr.(*[16]byte); ok {
		copy((*destArrayPtr)[:], uuid[:])
		return
	}
	destValue := reflect.ValueOf(destPtr).Elem()
	for i := 0; i < 16; i++ {
		destValue.Index(i).Set(reflect.ValueOf(uuid[i]))
	}
}

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

func callsite(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return ""
	}
	return filepath.Base(file) + ":" + strconv.Itoa(line) + ": "
}

type displayType int8

const (
	displayTypeBinary displayType = iota
	displayTypeString
	displayTypeUUID
)

// nullBytes is used in place of scanning into *[]byte. We use *nullBytes
// instead of *[]byte because of the displayType field, which determines how to
// render the value to the user. This is important for logging the query
// results, because UUIDs/JSON/Arrays are all scanned into bytes, but we don't
// want to display them as bytes (we need to convert them to UUID/JSON/Array
// strings instead).
type nullBytes struct {
	bytes       []byte
	dialect     string
	displayType displayType
	valid       bool
}

func (n *nullBytes) Scan(value any) error {
	if value == nil {
		n.bytes, n.valid = nil, false
		return nil
	}
	n.valid = true
	switch value := value.(type) {
	case string:
		n.bytes = []byte(value)
	case []byte:
		n.bytes = value
	default:
		return fmt.Errorf("unable to convert %#v to bytes", value)
	}
	return nil
}

func (n *nullBytes) Value() (driver.Value, error) {
	if !n.valid {
		return nil, nil
	}
	switch n.displayType {
	case displayTypeString:
		return string(n.bytes), nil
	case displayTypeUUID:
		if n.dialect != "postgres" {
			return n.bytes, nil
		}
		var uuid [16]byte
		var buf [36]byte
		copy(uuid[:], n.bytes)
		googleuuid.EncodeHex(buf[:], uuid)
		return string(buf[:]), nil
	default:
		return n.bytes, nil
	}
}

func markNoColumnIndexPanic(row *Row, format string) int {
	index, ok := row.columnIndex[format]
	if !ok {
		panic(fmt.Errorf(callsite(1)+"column %s does not exist (available columns: %s)", format, strings.Join(row.columns, ", ")))
	}
	return index
}

func markQueryIsStaticPanic(row *Row, target string) {
	if row.queryIsStatic {
		panic(fmt.Errorf("%s", callsite(1)+"cannot call "+target+" for static queries"))
	}
}
