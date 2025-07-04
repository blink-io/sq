package sq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Writef is a fmt.Sprintf-style function that will write a format string and
// values slice into an Output. The only recognized placeholder is '{}'.
// Placeholders can be anonymous (e.g. {}), ordinal (e.g. {1}, {2}, {3}) or
// named (e.g. {name}, {email}, {age}).
//
// - Anonymous placeholders refer to successive values in the values slice.
// Anonymous placeholders are treated like a series of incrementing ordinal
// placeholders.
//
// - Ordinal placeholders refer to a specific value in the values slice using
// 1-based indexing.
//
// - Named placeholders refer to their corresponding sql.NamedArg value in the
// values slice. If there are multiple sql.NamedArg values with the same name,
// the last one wins.
//
// If a value is an SQLWriter, its WriteSQL method will be called. Else if a
// value is a slice, it will undergo slice expansion
// (https://bokwoon.neocities.org/sq.html#value-expansion). Otherwise, the
// value is added to the query args slice.
func Writef(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, format string, values []any) error {
	return writef(ctx, dialect, buf, args, params, format, values, nil, nil)
}

func writef(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, format string, values []any, runningValuesIndex *int, ordinalIndex map[int]int) error {
	// optimized case when the format string does not contain any '{}'
	// placeholders
	if i := strings.IndexByte(format, '{'); i < 0 {
		buf.WriteString(format)
		return nil
	}

	// namedIndex tracks the indexes of the namedArgs that are inside the
	// values slice
	namedIndex := make(map[string]int)
	for i, value := range values {
		var name string
		switch arg := value.(type) {
		case sql.NamedArg:
			name = arg.Name
		case Parameter:
			name = arg.Name
		case ArrayParameter:
			name = arg.Name
		case BinaryParameter:
			name = arg.Name
		case BooleanParameter:
			name = arg.Name
		case EnumParameter:
			name = arg.Name
		case JSONParameter:
			name = arg.Name
		case NumberParameter:
			name = arg.Name
		case StringParameter:
			name = arg.Name
		case TimeParameter:
			name = arg.Name
		case UUIDParameter:
			name = arg.Name
		}
		if name != "" {
			if _, ok := namedIndex[name]; ok {
				return fmt.Errorf("named parameter {%s} provided more than once", name)
			}
			namedIndex[name] = i
		}
	}
	buf.Grow(len(format))
	if runningValuesIndex == nil {
		n := 0
		runningValuesIndex = &n
	}
	// ordinalIndex tracks the index of the ordinals that have already been
	// written into the args slice
	if ordinalIndex == nil {
		ordinalIndex = make(map[int]int)
	}

	// jump to each '{' character in the format string
	for i := strings.IndexByte(format, '{'); i >= 0; i = strings.IndexByte(format, '{') {
		// Unescape '{{' to '{'
		if i+1 <= len(format) && format[i+1] == '{' {
			buf.WriteString(format[:i])
			buf.WriteByte('{')
			format = format[i+2:]
			continue
		}
		buf.WriteString(format[:i])
		format = format[i:]

		// If we can't find the terminating '}' return an error
		j := strings.IndexByte(format, '}')
		if j < 0 {
			return fmt.Errorf("no '}' found")
		}

		paramName := format[1:j]
		format = format[j+1:]
		for _, char := range paramName {
			if char != '_' && !unicode.IsLetter(char) && !unicode.IsDigit(char) {
				return fmt.Errorf("%q is not a valid param name (only letters, digits and '_' are allowed)", paramName)
			}
		}

		// is it an anonymous placeholder? e.g. {}
		if paramName == "" {
			if *runningValuesIndex >= len(values) {
				return fmt.Errorf("too few values passed in to Writef, expected more than %d", runningValuesIndex)
			}
			value := values[*runningValuesIndex]
			*runningValuesIndex++
			err := WriteValue(ctx, dialect, buf, args, params, value)
			if err != nil {
				return err
			}
			continue
		}

		// is it an ordinal placeholder? e.g. {1}, {2}, {3}
		ordinal, err := strconv.Atoi(paramName)
		if err == nil {
			err = writeOrdinalValue(ctx, dialect, buf, args, params, values, ordinal, ordinalIndex)
			if err != nil {
				return err
			}
			continue
		}

		// is it a named placeholder? e.g. {name}, {age}, {email}
		index, ok := namedIndex[paramName]
		if !ok {
			availableParams := make([]string, 0, len(namedIndex))
			for name := range namedIndex {
				availableParams = append(availableParams, name)
			}
			sort.Strings(availableParams)
			return fmt.Errorf("named parameter {%s} not provided (available params: %s)", paramName, strings.Join(availableParams, ", "))
		}
		value := values[index]
		err = WriteValue(ctx, dialect, buf, args, params, value)
		if err != nil {
			return err
		}
	}
	buf.WriteString(format)
	return nil
}

// WriteValue is the equivalent of Writef but for writing a single value into
// the Output.
func WriteValue(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, value any) error {
	if namedArg, ok := value.(sql.NamedArg); ok {
		return writeNamedArg(ctx, dialect, buf, args, params, namedArg)
	}
	if w, ok := value.(SQLWriter); ok {
		return w.WriteSQL(ctx, dialect, buf, args, params)
	}
	if isExpandableSlice(value) {
		return expandSlice(ctx, dialect, buf, args, params, value)
	}
	value, err := preprocessValue(dialect, value)
	if err != nil {
		return err
	}
	*args = append(*args, value)
	index := len(*args) - 1
	switch dialect {
	case DialectPostgres, DialectSQLite:
		buf.WriteString("$" + strconv.Itoa(index+1))
	case DialectSQLServer:
		buf.WriteString("@p" + strconv.Itoa(index+1))
	default:
		buf.WriteString("?")
	}
	return nil
}

// QuoteIdentifier quotes an identifier if necessary using dialect-specific
// quoting rules.
func QuoteIdentifier(dialect string, identifier string) string {
	var needsQuoting bool
	switch identifier {
	case "":
		needsQuoting = true
	case "EXCLUDED", "INSERTED", "DELETED", "NEW", "OLD":
		needsQuoting = false
	default:
		for i, char := range identifier {
			if i == 0 && (char >= '0' && char <= '9') {
				// the first character cannot be a number
				needsQuoting = true
				break
			}
			if char == '_' || (char >= '0' && char <= '9') || (char >= 'a' && char <= 'z') {
				continue
			}
			// If there are capital letters, the identifier is quoted to preserve
			// capitalization information (because databases treat capital letters
			// differently based on their dialect or configuration).
			// If the character is anything else, we also quote. In general, there
			// may be some special characters that are allowed in unquoted
			// identifiers (e.g. '$'), but different databases allow different
			// things. We only recognize _a-z0-9 as the true standard.
			needsQuoting = true
			break
		}
		if !needsQuoting && dialect != "" {
			switch dialect {
			case DialectSQLite:
				_, needsQuoting = sqliteKeywords[strings.ToLower(identifier)]
			case DialectPostgres:
				_, needsQuoting = postgresKeywords[strings.ToLower(identifier)]
			case DialectMySQL:
				_, needsQuoting = mysqlKeywords[strings.ToLower(identifier)]
			case DialectSQLServer:
				_, needsQuoting = sqlserverKeywords[strings.ToLower(identifier)]
			}
		}
	}
	if !needsQuoting {
		return identifier
	}
	switch dialect {
	case DialectMySQL:
		return "`" + EscapeQuote(identifier, '`') + "`"
	case DialectSQLServer:
		return "[" + EscapeQuote(identifier, ']') + "]"
	default:
		return `"` + EscapeQuote(identifier, '"') + `"`
	}
}

// EscapeQuote will escape the relevant quote in a string by doubling up on it
// (as per SQL rules).
func EscapeQuote(str string, quote byte) string {
	i := strings.IndexByte(str, quote)
	if i < 0 {
		return str
	}
	var b strings.Builder
	b.Grow(len(str) + strings.Count(str, string(quote)))
	for i >= 0 {
		b.WriteString(str[:i])
		b.WriteByte(quote)
		b.WriteByte(quote)
		if len(str[i:]) > 2 && str[i] == quote && str[i+1] == quote {
			str = str[i+2:]
		} else {
			str = str[i+1:]
		}
		i = strings.IndexByte(str, quote)
	}
	b.WriteString(str)
	return b.String()
}

// Sprintf will interpolate SQL args into a query string containing prepared
// statement parameters. It returns an error if an argument cannot be properly
// represented in SQL. This function may be vulnerable to SQL injection and
// should be used for logging purposes only.
func Sprintf(dialect string, query string, args []any) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	buf.Grow(len(query))
	namedIndices := make(map[string]int)
	for i, arg := range args {
		switch arg := arg.(type) {
		case sql.NamedArg:
			namedIndices[arg.Name] = i
		}
	}
	runningArgsIndex := 0
	mustWriteCharAt := -1
	insideStringOrIdentifier := false
	var openingQuote rune
	var paramName []rune
	for i, char := range query {
		// do we unconditionally write in the current char?
		if mustWriteCharAt == i {
			buf.WriteRune(char)
			continue
		}
		// are we currently inside a string or identifier?
		if insideStringOrIdentifier {
			buf.WriteRune(char)
			switch openingQuote {
			case '\'', '"', '`':
				// does the current char terminate the current string or identifier?
				if char == openingQuote {
					// is the next char the same as the current char, which
					// escapes it and prevents it from terminating the current
					// string or identifier?
					if i+1 < len(query) && rune(query[i+1]) == openingQuote {
						mustWriteCharAt = i + 1
					} else {
						insideStringOrIdentifier = false
					}
				}
			case '[':
				// does the current char terminate the current string or identifier?
				if char == ']' {
					// is the next char the same as the current char, which
					// escapes it and prevents it from terminating the current
					// string or identifier?
					if i+1 < len(query) && query[i+1] == ']' {
						mustWriteCharAt = i + 1
					} else {
						insideStringOrIdentifier = false
					}
				}
			}
			continue
		}
		// does the current char mark the start of a new string or identifier?
		if char == '\'' || char == '"' || (char == '`' && dialect == DialectMySQL) || (char == '[' && dialect == DialectSQLServer) {
			insideStringOrIdentifier = true
			openingQuote = char
			buf.WriteRune(char)
			continue
		}
		// are we currently inside a parameter name?
		if len(paramName) > 0 {
			// does the current char terminate the current parameter name?
			if char != '_' && !unicode.IsLetter(char) && !unicode.IsDigit(char) {
				paramValue, err := lookupParam(dialect, args, paramName, namedIndices, runningArgsIndex)
				if err != nil {
					return buf.String(), err
				}
				buf.WriteString(paramValue)
				buf.WriteRune(char)
				if len(paramName) == 1 && paramName[0] == '?' {
					runningArgsIndex++
				}
				paramName = paramName[:0]
			} else {
				paramName = append(paramName, char)
			}
			continue
		}
		// does the current char mark the start of a new parameter name?
		if (char == '$' && (dialect == DialectSQLite || dialect == DialectPostgres)) ||
			(char == ':' && dialect == DialectSQLite) ||
			(char == '@' && (dialect == DialectSQLite || dialect == DialectSQLServer)) {
			paramName = append(paramName, char)
			continue
		}
		// is the current char the anonymous '?' parameter?
		if char == '?' && dialect != DialectPostgres {
			// for sqlite, just because we encounter a '?' doesn't mean it
			// is an anonymous param. sqlite also supports using '?' for
			// ordinal params (e.g. ?1, ?2, ?3) or named params (?foo,
			// ?bar, ?baz). Hence we treat it as an ordinal/named param
			// first, and handle the edge case later when it isn't.
			if dialect == DialectSQLite {
				paramName = append(paramName, char)
				continue
			}
			if runningArgsIndex >= len(args) {
				return buf.String(), fmt.Errorf("too few args provided, expected more than %d", runningArgsIndex+1)
			}
			paramValue, err := Sprint(dialect, args[runningArgsIndex])
			if err != nil {
				return buf.String(), err
			}
			buf.WriteString(paramValue)
			runningArgsIndex++
			continue
		}
		// if all the above questions answer false, we just write the current
		// char in and continue
		buf.WriteRune(char)
	}
	// flush the paramName buffer (to handle edge case where the query ends with a parameter name)
	if len(paramName) > 0 {
		paramValue, err := lookupParam(dialect, args, paramName, namedIndices, runningArgsIndex)
		if err != nil {
			return buf.String(), err
		}
		buf.WriteString(paramValue)
	}
	if insideStringOrIdentifier {
		return buf.String(), fmt.Errorf("unclosed string or identifier")
	}
	return buf.String(), nil
}

// Sprint is the equivalent of Sprintf but for converting a single value into
// its SQL representation.
func Sprint(dialect string, v any) (string, error) {
	const (
		timestamp             = "2006-01-02 15:04:05"
		timestampWithTimezone = "2006-01-02 15:04:05.9999999-07:00"
	)
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if v {
			if dialect == DialectSQLServer {
				return "1", nil
			}
			return "TRUE", nil
		}
		if dialect == DialectSQLServer {
			return "0", nil
		}
		return "FALSE", nil
	case []byte:
		switch dialect {
		case DialectPostgres:
			// https://www.postgresql.org/docs/current/datatype-binary.html
			// (see 8.4.1. bytea Hex Format)
			return `'\x` + hex.EncodeToString(v) + `'`, nil
		case DialectSQLServer:
			return `0x` + hex.EncodeToString(v), nil
		default:
			return `x'` + hex.EncodeToString(v) + `'`, nil
		}
	case string:
		str := v
		i := strings.IndexAny(str, "\r\n")
		if i < 0 {
			return `'` + strings.ReplaceAll(str, `'`, `''`) + `'`, nil
		}
		var b strings.Builder
		if dialect == DialectMySQL || dialect == DialectSQLServer {
			b.WriteString("CONCAT(")
		}
		for i >= 0 {
			if str[:i] != "" {
				b.WriteString(`'` + strings.ReplaceAll(str[:i], `'`, `''`) + `'`)
				if dialect == DialectMySQL || dialect == DialectSQLServer {
					b.WriteString(", ")
				} else {
					b.WriteString(" || ")
				}
			}
			switch str[i] {
			case '\r':
				if dialect == DialectPostgres {
					b.WriteString("CHR(13)")
				} else {
					b.WriteString("CHAR(13)")
				}
			case '\n':
				if dialect == DialectPostgres {
					b.WriteString("CHR(10)")
				} else {
					b.WriteString("CHAR(10)")
				}
			}
			if str[i+1:] != "" {
				if dialect == DialectMySQL || dialect == DialectSQLServer {
					b.WriteString(", ")
				} else {
					b.WriteString(" || ")
				}
			}
			str = str[i+1:]
			i = strings.IndexAny(str, "\r\n")
		}
		if str != "" {
			b.WriteString(`'` + strings.ReplaceAll(str, `'`, `''`) + `'`)
		}
		if dialect == DialectMySQL || dialect == DialectSQLServer {
			b.WriteString(")")
		}
		return b.String(), nil
	case time.Time:
		if dialect == DialectPostgres || dialect == DialectSQLServer {
			return `'` + v.Format(timestampWithTimezone) + `'`, nil
		}
		return `'` + v.UTC().Format(timestamp) + `'`, nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int8:
		return strconv.FormatInt(int64(v), 10), nil
	case int16:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 64), nil
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case sql.NamedArg:
		return Sprint(dialect, v.Value)
	case sql.NullBool:
		if !v.Valid {
			return "NULL", nil
		}
		if v.Bool {
			if dialect == DialectSQLServer {
				return "1", nil
			}
			return "TRUE", nil
		}
		if dialect == DialectSQLServer {
			return "0", nil
		}
		return "FALSE", nil
	case sql.NullFloat64:
		if !v.Valid {
			return "NULL", nil
		}
		return strconv.FormatFloat(v.Float64, 'g', -1, 64), nil
	case sql.NullInt64:
		if !v.Valid {
			return "NULL", nil
		}
		return strconv.FormatInt(v.Int64, 10), nil
	case sql.NullInt32:
		if !v.Valid {
			return "NULL", nil
		}
		return strconv.FormatInt(int64(v.Int32), 10), nil
	case sql.NullString:
		if !v.Valid {
			return "NULL", nil
		}
		return Sprint(dialect, v.String)
	case sql.NullTime:
		if !v.Valid {
			return "NULL", nil
		}
		if dialect == DialectPostgres || dialect == DialectSQLServer {
			return `'` + v.Time.Format(timestampWithTimezone) + `'`, nil
		}
		return `'` + v.Time.UTC().Format(timestamp) + `'`, nil
	case driver.Valuer:
		vv, err := v.Value()
		if err != nil {
			return "", fmt.Errorf("error when calling Value(): %w", err)
		}
		switch vv.(type) {
		case int64, float64, bool, []byte, string, time.Time, nil:
			return Sprint(dialect, vv)
		default:
			return "", fmt.Errorf("invalid driver.Value type %T (must be one of int64, float64, bool, []byte, string, time.Time, nil)", vv)
		}
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
		if !rv.IsValid() {
			return "NULL", nil
		}
	}
	switch v := rv.Interface().(type) {
	case bool, []byte, string, time.Time, int, int8, int16, int32, int64, uint,
		uint8, uint16, uint32, uint64, float32, float64, sql.NamedArg,
		sql.NullBool, sql.NullFloat64, sql.NullInt64, sql.NullInt32,
		sql.NullString, sql.NullTime, driver.Valuer:
		return Sprint(dialect, v)
	default:
		return "", fmt.Errorf("%T has no SQL representation", v)
	}
}

// isExpandableSlice checks if a value is an expandable slice.
func isExpandableSlice(value any) bool {
	// treat byte slices as a special case that we never want to expand
	if _, ok := value.([]byte); ok {
		return false
	}
	valueType := reflect.TypeOf(value)
	if valueType == nil {
		return false
	}
	return valueType.Kind() == reflect.Slice
}

// expandSlice expands a slice value into Output. Make sure the value is an
// expandable slice first by checking it with isExpandableSlice().
func expandSlice(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, value any) error {
	slice := reflect.ValueOf(value)
	var err error
	for i := 0; i < slice.Len(); i++ {
		if i > 0 {
			buf.WriteString(", ")
		}
		arg := slice.Index(i).Interface()
		if v, ok := arg.(SQLWriter); ok {
			err = v.WriteSQL(ctx, dialect, buf, args, params)
			if err != nil {
				return err
			}
			continue
		}
		switch dialect {
		case DialectPostgres, DialectSQLite:
			buf.WriteString("$" + strconv.Itoa(len(*args)+1))
		case DialectSQLServer:
			buf.WriteString("@p" + strconv.Itoa(len(*args)+1))
		default:
			buf.WriteString("?")
		}
		arg, err = preprocessValue(dialect, arg)
		if err != nil {
			return err
		}
		*args = append(*args, arg)
	}
	return nil
}

// writeNamedArg writes a sql.NamedArg into the Output.
func writeNamedArg(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, namedArg sql.NamedArg) error {
	if w, ok := namedArg.Value.(SQLWriter); ok {
		return w.WriteSQL(ctx, dialect, buf, args, params)
	}
	if isExpandableSlice(namedArg.Value) {
		return expandSlice(ctx, dialect, buf, args, params, namedArg.Value)
	}
	var err error
	namedArg.Value, err = preprocessValue(dialect, namedArg.Value)
	if err != nil {
		return err
	}
	paramIndices := params[namedArg.Name]
	if len(paramIndices) > 0 {
		index := paramIndices[0]
		switch dialect {
		case DialectSQLite:
			(*args)[index] = namedArg
			buf.WriteString("$" + namedArg.Name)
			return nil
		case DialectPostgres:
			(*args)[index] = namedArg.Value
			buf.WriteString("$" + strconv.Itoa(index+1))
			return nil
		case DialectSQLServer:
			(*args)[index] = namedArg
			buf.WriteString("@" + namedArg.Name)
			return nil
		default:
			for _, index := range paramIndices {
				(*args)[index] = namedArg.Value
			}
		}
	}
	switch dialect {
	case DialectSQLite:
		*args = append(*args, namedArg)
		if params != nil {
			index := len(*args) - 1
			params[namedArg.Name] = []int{index}
		}
		buf.WriteString("$" + namedArg.Name)
	case DialectPostgres:
		*args = append(*args, namedArg.Value)
		index := len(*args) - 1
		if params != nil {
			params[namedArg.Name] = []int{index}
		}
		buf.WriteString("$" + strconv.Itoa(index+1))
	case DialectSQLServer:
		*args = append(*args, namedArg)
		if params != nil {
			index := len(*args) - 1
			params[namedArg.Name] = []int{index}
		}
		buf.WriteString("@" + namedArg.Name)
	default:
		*args = append(*args, namedArg.Value)
		if params != nil {
			index := len(*args) - 1
			params[namedArg.Name] = append(paramIndices, index)
		}
		buf.WriteString("?")
	}
	return nil
}

// writeOrdinalValue writes an ordinal value into the Output. The
// ordinalIndices map is there to keep track of which ordinal values we have
// already appended to args (which we do not want to append again).
func writeOrdinalValue(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int, values []any, ordinal int, ordinalIndices map[int]int) error {
	index := ordinal - 1
	if index < 0 || index >= len(values) {
		return fmt.Errorf("ordinal parameter {%d} is out of bounds", ordinal)
	}
	value := values[index]
	if namedArg, ok := value.(sql.NamedArg); ok {
		return writeNamedArg(ctx, dialect, buf, args, params, namedArg)
	}
	if w, ok := value.(SQLWriter); ok {
		return w.WriteSQL(ctx, dialect, buf, args, params)
	}
	if isExpandableSlice(value) {
		return expandSlice(ctx, dialect, buf, args, params, value)
	}
	var err error
	value, err = preprocessValue(dialect, value)
	if err != nil {
		return err
	}
	switch dialect {
	case DialectSQLite, DialectPostgres, DialectSQLServer:
		index, ok := ordinalIndices[ordinal]
		if !ok {
			*args = append(*args, value)
			index = len(*args) - 1
			ordinalIndices[ordinal] = index
		}
		switch dialect {
		case DialectSQLite, DialectPostgres:
			buf.WriteString("$" + strconv.Itoa(index+1))
		case DialectSQLServer:
			buf.WriteString("@p" + strconv.Itoa(index+1))
		}
	default:
		err := WriteValue(ctx, dialect, buf, args, params, value)
		if err != nil {
			return err
		}
	}
	return nil
}

// lookupParam returns the SQL representation of a paramName (inside the args
// slice).
func lookupParam(dialect string, args []any, paramName []rune, namedIndices map[string]int, runningArgsIndex int) (paramValue string, err error) {
	var maybeNum string
	if paramName[0] == '@' && dialect == DialectSQLServer && len(paramName) >= 2 && (paramName[1] == 'p' || paramName[1] == 'P') {
		maybeNum = string(paramName[2:])
	} else {
		maybeNum = string(paramName[1:])
	}

	// is paramName an anonymous parameter?
	if maybeNum == "" {
		if paramName[0] != '?' {
			return "", fmt.Errorf("parameter name missing")
		}
		paramValue, err = Sprint(dialect, args[runningArgsIndex])
		if err != nil {
			return "", err
		}
		return paramValue, nil
	}

	// is paramName an ordinal paramater?
	ordinal, err := strconv.Atoi(maybeNum)
	if err == nil {
		index := ordinal - 1
		if index < 0 || index >= len(args) {
			return "", fmt.Errorf("args index %d out of bounds", ordinal)
		}
		paramValue, err = Sprint(dialect, args[index])
		if err != nil {
			return "", err
		}
		return paramValue, nil
	}

	// if we reach here, we know that the paramName is not an ordinal parameter
	// i.e. it is a named parameter
	if dialect == DialectPostgres || dialect == DialectMySQL {
		return "", fmt.Errorf("%s does not support %s named parameter", dialect, string(paramName))
	}
	index, ok := namedIndices[string(paramName[1:])]
	if !ok {
		return "", fmt.Errorf("named parameter %s not provided", string(paramName))
	}
	if index < 0 || index >= len(args) {
		return "", fmt.Errorf("args index %d out of bounds", ordinal)
	}
	paramValue, err = Sprint(dialect, args[index])
	if err != nil {
		return "", err
	}
	return paramValue, nil
}

func quoteTableColumns(dialect string, table Table) string {
	tableWithColumns, ok := table.(interface{ GetColumns() []string })
	if !ok {
		return ""
	}
	columns := tableWithColumns.GetColumns()
	if len(columns) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString(" (")
	for i, column := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(QuoteIdentifier(dialect, column))
	}
	b.WriteString(")")
	return b.String()
}

// Params is a shortcut for typing map[string]any.
type Params = map[string]any

// Parameter is identical to sql.NamedArg, but implements the Field interface.
type Parameter sql.NamedArg

var _ Field = (*Parameter)(nil)

// Param creates a new Parameter.
func Param(name string, value any) Parameter {
	return Parameter{Name: name, Value: value}
}

// WriteSQL implements the SQLWriter interface.
func (p Parameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p Parameter) IsField() {}

// ArrayParameter is identical to sql.NamedArg, but implements the Array interface.
type ArrayParameter sql.NamedArg

var _ Field = (*ArrayParameter)(nil)

// ArrayParam creates a new ArrayParameter. It wraps the value with
// ArrayValue().
func ArrayParam(name string, value any) ArrayParameter {
	return ArrayParameter{Name: name, Value: ArrayValue(value)}
}

// WriteSQL implements the SQLWriter interface.
func (p ArrayParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p ArrayParameter) IsField() {}

// IsArray implements the Array interface.
func (p ArrayParameter) IsArray() {}

// BinaryParameter is identical to sql.NamedArg, but implements the Binary
// interface.
type BinaryParameter sql.NamedArg

var _ Binary = (*BinaryParameter)(nil)

// BytesParam creates a new BinaryParameter using a []byte value.
func BytesParam(name string, b []byte) BinaryParameter {
	return BinaryParameter{Name: name, Value: b}
}

// WriteSQL implements the SQLWriter interface.
func (p BinaryParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p BinaryParameter) IsField() {}

// IsBinary implements the Binary interface.
func (p BinaryParameter) IsBinary() {}

// BooleanParameter is identical to sql.NamedArg, but implements the Boolean
// interface.
type BooleanParameter sql.NamedArg

var _ Boolean = (*BooleanParameter)(nil)

// BoolParam creates a new BooleanParameter from a bool value.
func BoolParam(name string, b bool) BooleanParameter {
	return BooleanParameter{Name: name, Value: b}
}

// WriteSQL implements the SQLWriter interface.
func (p BooleanParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p BooleanParameter) IsField() {}

// IsBoolean implements the Boolean interface.
func (p BooleanParameter) IsBoolean() {}

// EnumParameter is identical to sql.NamedArg, but implements the Enum
// interface.
type EnumParameter sql.NamedArg

var _ Field = (*EnumParameter)(nil)

// EnumParam creates a new EnumParameter. It wraps the value with EnumValue().
func EnumParam(name string, value Enumeration) EnumParameter {
	return EnumParameter{Name: name, Value: EnumValue(value)}
}

// WriteSQL implements the SQLWriter interface.
func (p EnumParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p EnumParameter) IsField() {}

// IsEnum implements the Enum interface.
func (p EnumParameter) IsEnum() {}

// JSONParameter is identical to sql.NamedArg, but implements the JSON
// interface.
type JSONParameter sql.NamedArg

var _ Field = (*JSONParameter)(nil)

// JSONParam creates a new JSONParameter. It wraps the value with JSONValue().
func JSONParam(name string, value any) JSONParameter {
	return JSONParameter{Name: name, Value: JSONValue(value)}
}

// WriteSQL implements the SQLWriter interface.
func (p JSONParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p JSONParameter) IsField() {}

// IsJSON implements the JSON interface.
func (p JSONParameter) IsJSON() {}

// NumberParameter is identical to sql.NamedArg, but implements the Number
// interface.
type NumberParameter sql.NamedArg

var _ Number = (*NumberParameter)(nil)

// IntParam creates a new NumberParameter from the int value.
func IntParam(name string, num int) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Int8Param creates a new NumberParameter from the int8 value.
func Int8Param(name string, num int8) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Int16Param creates a new NumberParameter from the int16 value.
func Int16Param(name string, num int16) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Int32Param creates a new NumberParameter from the int32 value.
func Int32Param(name string, num int32) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Int64Param creates a new NumberParameter from the int64 value.
func Int64Param(name string, num int64) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// UintParam creates a new NumberParameter from the uint value.
func UintParam(name string, num uint) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Uint8Param creates a new NumberParameter from the int8 value.
func Uint8Param(name string, num uint8) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Uint16Param creates a new NumberParameter from the int16 value.
func Uint16Param(name string, num uint16) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Uint32Param creates a new NumberParameter from the uint32 value.
func Uint32Param(name string, num uint32) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Uint64Param creates a new NumberParameter from the uint64 value.
func Uint64Param(name string, num uint64) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Float32Param creates a new NumberParameter from the float32 value.
func Float32Param(name string, num float32) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// Float64Param creates a new NumberParameter from the float64 value.
func Float64Param(name string, num float64) NumberParameter {
	return NumberParameter{Name: name, Value: num}
}

// WriteSQL implements the SQLWriter interface.
func (p NumberParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p NumberParameter) IsField() {}

// IsNumber implements the Number interface.
func (p NumberParameter) IsNumber() {}

// StringParameter is identical to sql.NamedArg, but implements the String
// interface.
type StringParameter sql.NamedArg

var _ String = (*StringParameter)(nil)

// StringParam creates a new StringParameter from a string value.
func StringParam(name string, s string) StringParameter {
	return StringParameter{Name: name, Value: s}
}

// WriteSQL implements the SQLWriter interface.
func (p StringParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p StringParameter) IsField() {}

// IsString implements the String interface.
func (p StringParameter) IsString() {}

// TimeParameter is identical to sql.NamedArg, but implements the Time
// interface.
type TimeParameter sql.NamedArg

var _ Time = (*TimeParameter)(nil)

// TimeParam creates a new TimeParameter from a time.Time value.
func TimeParam(name string, t time.Time) TimeParameter {
	return TimeParameter{Name: name, Value: t}
}

// WriteSQL implements the SQLWriter interface.
func (p TimeParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p TimeParameter) IsField() {}

// IsTime implements the Time interface.
func (p TimeParameter) IsTime() {}

// UUIDParameter is identical to sql.NamedArg, but implements the UUID
// interface.
type UUIDParameter sql.NamedArg

var _ Field = (*UUIDParameter)(nil)

// UUIDParam creates a new UUIDParameter. It wraps the value with UUIDValue().
func UUIDParam(name string, value any) UUIDParameter {
	return UUIDParameter{Name: name, Value: UUIDValue(value)}
}

// WriteSQL implements the SQLWriter interface.
func (p UUIDParameter) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return writeNamedArg(ctx, dialect, buf, args, params, sql.NamedArg(p))
}

// IsField implements the Field interface.
func (p UUIDParameter) IsField() {}

// IsUUID implements the UUID interface.
func (p UUIDParameter) IsUUID() {}

// SQLite keyword reference: https://www.sqlite.org/lang_keywords.html
var sqliteKeywords = map[string]struct{}{
	"abort": {}, "action": {}, "add": {}, "after": {}, "all": {}, "alter": {},
	"always": {}, "analyze": {}, "and": {}, "as": {}, "asc": {}, "attach": {},
	"autoincrement": {}, "before": {}, "begin": {}, "between": {}, "by": {},
	"cascade": {}, "case": {}, "cast": {}, "check": {}, "collate": {}, "column": {},
	"commit": {}, "conflict": {}, "constraint": {}, "create": {}, "cross": {},
	"current": {}, "current_date": {}, "current_time": {}, "current_timestamp": {},
	"database": {}, "default": {}, "deferrable": {}, "deferred": {}, "delete": {},
	"desc": {}, "detach": {}, "distinct": {}, "do": {}, "drop": {}, "each": {},
	"else": {}, "end": {}, "escape": {}, "except": {}, "exclude": {}, "exclusive": {},
	"exists": {}, "explain": {}, "fail": {}, "filter": {}, "first": {}, "following": {},
	"for": {}, "foreign": {}, "from": {}, "full": {}, "generated": {}, "glob": {},
	"group": {}, "groups": {}, "having": {}, "if": {}, "ignore": {}, "immediate": {},
	"in": {}, "index": {}, "indexed": {}, "initially": {}, "inner": {}, "insert": {},
	"instead": {}, "intersect": {}, "into": {}, "is": {}, "isnull": {}, "join": {},
	"key": {}, "last": {}, "left": {}, "like": {}, "limit": {}, "match": {},
	"materialized": {}, "natural": {}, "no": {}, "not": {}, "nothing": {}, "notnull": {},
	"null": {}, "nulls": {}, "of": {}, "offset": {}, "on": {}, "or": {}, "order": {},
	"others": {}, "outer": {}, "over": {}, "partition": {}, "plan": {}, "pragma": {},
	"preceding": {}, "primary": {}, "query": {}, "raise": {}, "range": {},
	"recursive": {}, "references": {}, "regexp": {}, "reindex": {}, "release": {},
	"rename": {}, "replace": {}, "restrict": {}, "returning": {}, "right": {},
	"rollback": {}, "row": {}, "rows": {}, "savepoint": {}, "select": {}, "set": {},
	"table": {}, "temp": {}, "temporary": {}, "then": {}, "ties": {}, "to": {},
	"transaction": {}, "trigger": {}, "unbounded": {}, "union": {}, "unique": {},
	"update": {}, "using": {}, "vacuum": {}, "values": {}, "view": {}, "virtual": {},
	"when": {}, "where": {}, "window": {}, "with": {}, "without": {},
}

// Postgres keyword reference:
// https://www.postgresql.org/docs/current/sql-keywords-appendix.html
var postgresKeywords = map[string]struct{}{
	"all": {}, "analyse": {}, "analyze": {}, "and": {}, "any": {}, "array": {}, "as": {},
	"asc": {}, "asymmetric": {}, "authorization": {}, "binary": {}, "both": {},
	"case": {}, "cast": {}, "check": {}, "collate": {}, "collation": {}, "column": {},
	"concurrently": {}, "constraint": {}, "create": {}, "cross": {},
	"current_catalog": {}, "current_date": {}, "current_role": {},
	"current_schema": {}, "current_time": {}, "current_timestamp": {},
	"current_user": {}, "default": {}, "deferrable": {}, "desc": {}, "distinct": {},
	"do": {}, "else": {}, "end": {}, "except": {}, "false": {}, "fetch": {}, "for": {},
	"foreign": {}, "freeze": {}, "from": {}, "full": {}, "grant": {}, "group": {},
	"having": {}, "ilike": {}, "in": {}, "initially": {}, "inner": {}, "intersect": {},
	"into": {}, "is": {}, "isnull": {}, "join": {}, "lateral": {}, "leading": {},
	"left": {}, "like": {}, "limit": {}, "localtime": {}, "localtimestamp": {},
	"natural": {}, "not": {}, "notnull": {}, "null": {}, "offset": {}, "on": {},
	"only": {}, "or": {}, "order": {}, "outer": {}, "overlaps": {}, "placing": {},
	"primary": {}, "references": {}, "returning": {}, "right": {}, "select": {},
	"session_user": {}, "similar": {}, "some": {}, "symmetric": {}, "table": {},
	"tablesample": {}, "then": {}, "to": {}, "trailing": {}, "true": {}, "union": {},
	"unique": {}, "user": {}, "using": {}, "variadic": {}, "verbose": {}, "when": {},
	"where": {}, "window": {}, "with": {},
}

// MySQL keyword reference:
// https://dev.mysql.com/doc/refman/8.0/en/keywords.html
var mysqlKeywords = map[string]struct{}{
	"accessible": {}, "add": {}, "all": {}, "alter": {}, "analyze": {}, "and": {},
	"as": {}, "asc": {}, "asensitive": {}, "before": {}, "between": {}, "bigint": {},
	"binary": {}, "blob": {}, "both": {}, "by": {}, "call": {}, "cascade": {}, "case": {},
	"change": {}, "char": {}, "character": {}, "check": {}, "collate": {}, "column": {},
	"condition": {}, "constraint": {}, "continue": {}, "convert": {}, "create": {},
	"cross": {}, "cube": {}, "cume_dist": {}, "current_date": {}, "current_time": {},
	"current_timestamp": {}, "current_user": {}, "cursor": {}, "database": {},
	"databases": {}, "day_hour": {}, "day_microsecond": {}, "day_minute": {},
	"day_second": {}, "dec": {}, "decimal": {}, "declare": {}, "default": {},
	"delayed": {}, "delete": {}, "dense_rank": {}, "desc": {}, "describe": {},
	"deterministic": {}, "distinct": {}, "distinctrow": {}, "div": {}, "double": {},
	"drop": {}, "dual": {}, "each": {}, "else": {}, "elseif": {}, "empty": {},
	"enclosed": {}, "escaped": {}, "except": {}, "exists": {}, "exit": {}, "explain": {},
	"false": {}, "fetch": {}, "first_value": {}, "float": {}, "float4": {}, "float8": {},
	"for": {}, "force": {}, "foreign": {}, "from": {}, "fulltext": {}, "function": {},
	"generated": {}, "get": {}, "grant": {}, "group": {}, "grouping": {}, "groups": {},
	"having": {}, "high_priority": {}, "hour_microsecond": {}, "hour_minute": {},
	"hour_second": {}, "if": {}, "ignore": {}, "in": {}, "index": {}, "infile": {},
	"inner": {}, "inout": {}, "insensitive": {}, "insert": {}, "int": {}, "int1": {},
	"int2": {}, "int3": {}, "int4": {}, "int8": {}, "integer": {}, "intersect": {},
	"interval": {}, "into": {}, "io_after_gtids": {}, "io_before_gtids": {}, "is": {},
	"iterate": {}, "join": {}, "json_table": {}, "key": {}, "keys": {}, "kill": {},
	"lag": {}, "last_value": {}, "lateral": {}, "lead": {}, "leading": {}, "leave": {},
	"left": {}, "like": {}, "limit": {}, "linear": {}, "lines": {}, "load": {},
	"localtime": {}, "localtimestamp": {}, "lock": {}, "long": {}, "longblob": {},
	"longtext": {}, "loop": {}, "low_priority": {}, "master_bind": {},
	"master_ssl_verify_server_cert": {}, "match": {}, "maxvalue": {}, "mediumblob": {},
	"mediumint": {}, "mediumtext": {}, "middleint": {}, "minute_microsecond": {},
	"minute_second": {}, "mod": {}, "modifies": {}, "natural": {}, "not": {},
	"no_write_to_binlog": {}, "nth_value": {}, "ntile": {}, "null": {}, "numeric": {},
	"of": {}, "on": {}, "optimize": {}, "optimizer_costs": {}, "option": {},
	"optionally": {}, "or": {}, "order": {}, "out": {}, "outer": {}, "outfile": {},
	"over": {}, "partition": {}, "percent_rank": {}, "precision": {}, "primary": {},
	"procedure": {}, "purge": {}, "range": {}, "rank": {}, "read": {}, "reads": {},
	"read_write": {}, "real": {}, "recursive": {}, "references": {}, "regexp": {},
	"release": {}, "rename": {}, "repeat": {}, "replace": {}, "require": {},
	"resignal": {}, "restrict": {}, "return": {}, "revoke": {}, "right": {}, "rlike": {},
	"row": {}, "rows": {}, "row_number": {}, "schema": {}, "schemas": {},
	"second_microsecond": {}, "select": {}, "sensitive": {}, "separator": {}, "set": {},
	"show": {}, "signal": {}, "smallint": {}, "spatial": {}, "specific": {}, "sql": {},
	"sqlexception": {}, "sqlstate": {}, "sqlwarning": {}, "sql_big_result": {},
	"sql_calc_found_rows": {}, "sql_small_result": {}, "ssl": {}, "starting": {},
	"stored": {}, "straight_join": {}, "system": {}, "table": {}, "terminated": {},
	"then": {}, "tinyblob": {}, "tinyint": {}, "tinytext": {}, "to": {}, "trailing": {},
	"trigger": {}, "true": {}, "undo": {}, "union": {}, "unique": {}, "unlock": {},
	"unsigned": {}, "update": {}, "usage": {}, "use": {}, "using": {}, "utc_date": {},
	"utc_time": {}, "utc_timestamp": {}, "values": {}, "varbinary": {}, "varchar": {},
	"varcharacter": {}, "varying": {}, "virtual": {}, "when": {}, "where": {},
	"while": {}, "window": {}, "with": {}, "write": {}, "xor": {}, "year_month": {},
	"zerofill": {},
}

// SQLServer keyword reference:
// https://learn.microsoft.com/en-us/sql/t-sql/language-elements/reserved-keywords-transact-sql?view=sql-server-ver16
var sqlserverKeywords = map[string]struct{}{
	"add": {}, "external": {}, "procedure": {}, "all": {}, "fetch": {}, "public": {},
	"alter": {}, "file": {}, "raiserror": {}, "and": {}, "fillfactor": {}, "read": {},
	"any": {}, "for": {}, "readtext": {}, "as": {}, "foreign": {}, "reconfigure": {},
	"asc": {}, "freetext": {}, "references": {}, "authorization": {},
	"freetexttable": {}, "replication": {}, "backup": {}, "from": {}, "restore": {},
	"begin": {}, "full": {}, "restrict": {}, "between": {}, "function": {}, "return": {},
	"break": {}, "goto": {}, "revert": {}, "browse": {}, "grant": {}, "revoke": {},
	"bulk": {}, "group": {}, "right": {}, "by": {}, "having": {}, "rollback": {},
	"cascade": {}, "holdlock": {}, "rowcount": {}, "case": {}, "identity": {},
	"rowguidcol": {}, "check": {}, "identity_insert": {}, "rule": {}, "checkpoint": {},
	"identitycol": {}, "save": {}, "close": {}, "if": {}, "schema": {}, "clustered": {},
	"in": {}, "securityaudit": {}, "coalesce": {}, "index": {}, "select": {},
	"collate": {}, "inner": {}, "semantickeyphrasetable": {}, "column": {},
	"insert": {}, "semanticsimilaritydetailstable": {}, "commit": {}, "intersect": {},
	"semanticsimilaritytable": {}, "compute": {}, "into": {}, "session_user": {},
	"constraint": {}, "is": {}, "set": {}, "contains": {}, "join": {}, "setuser": {},
	"containstable": {}, "key": {}, "shutdown": {}, "continue": {}, "kill": {},
	"some": {}, "convert": {}, "left": {}, "statistics": {}, "create": {}, "like": {},
	"system_user": {}, "cross": {}, "lineno": {}, "table": {}, "current": {}, "load": {},
	"tablesample": {}, "current_date": {}, "merge": {}, "textsize": {},
	"current_time": {}, "national": {}, "then": {}, "current_timestamp": {},
	"nocheck": {}, "to": {}, "current_user": {}, "nonclustered": {}, "top": {},
	"cursor": {}, "not": {}, "tran": {}, "database": {}, "null": {}, "transaction": {},
	"dbcc": {}, "nullif": {}, "trigger": {}, "deallocate": {}, "of": {}, "truncate": {},
	"declare": {}, "off": {}, "try_convert": {}, "default": {}, "offsets": {},
	"tsequal": {}, "delete": {}, "on": {}, "union": {}, "deny": {}, "open": {},
	"unique": {}, "desc": {}, "opendatasource": {}, "unpivot": {}, "disk": {},
	"openquery": {}, "update": {}, "distinct": {}, "openrowset": {}, "updatetext": {},
	"distributed": {}, "openxml": {}, "use": {}, "double": {}, "option": {}, "user": {},
	"drop": {}, "or": {}, "values": {}, "dump": {}, "order": {}, "varying": {},
	"else": {}, "outer": {}, "view": {}, "end": {}, "over": {}, "waitfor": {},
	"errlvl": {}, "percent": {}, "when": {}, "escape": {}, "pivot": {}, "where": {},
	"except": {}, "plan": {}, "while": {}, "exec": {}, "precision": {}, "with": {},
	"execute": {}, "primary": {}, "within group": {}, "exists": {}, "print": {},
	"writetext": {}, "exit": {}, "proc": {},
}
