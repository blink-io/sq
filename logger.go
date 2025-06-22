package sq

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[91m"
	colorGreen  = "\x1b[92m"
	colorYellow = "\x1b[93m"
	colorBlue   = "\x1b[94m"
	colorPurple = "\x1b[95m"
	colorCyan   = "\x1b[96m"
	colorGray   = "\x1b[97m"
	colorWhite  = "\x1b[97m"
)

// QueryStats represents the statistics from running a query.
type QueryStats struct {
	// Dialect of the query.
	Dialect string

	// Query string.
	Query string

	// Args slice provided with the query string.
	Args []any

	// Params maps param names back to arguments in the args slice (by index).
	Params map[string][]int

	// Err is the error from running the query.
	Err error

	// RowCount from running the query. Not valid for Exec().
	RowCount sql.NullInt64

	// RowsAffected by running the query. Not valid for
	// FetchOne/FetchAll/FetchCursor.
	RowsAffected sql.NullInt64

	// LastInsertId of the query.
	LastInsertId sql.NullInt64

	// Exists is the result of FetchExists().
	Exists sql.NullBool

	// When the query started at.
	StartedAt time.Time

	// Time taken by the query.
	TimeTaken time.Duration

	// The caller file where the query was invoked.
	CallerFile string

	// The line in the caller file that invoked the query.
	CallerLine int

	// The name of the function where the query was invoked.
	CallerFunction string

	// The results from running the query (if it was provided).
	Results string
}

// LogSettings are the various log settings taken into account when producing
// the QueryStats.
type LogSettings struct {
	// Dispatch logging asynchronously (logs may arrive out of order which can be confusing, but it won't block function calls).
	LogAsynchronously bool

	// Include time taken by the query.
	IncludeTime bool

	// Include caller (filename and line number).
	IncludeCaller bool

	// Include fetched results.
	IncludeResults int
}

// Logger represents a logger for the sq package.
type Logger interface {
	// LogSettings should populate a LogSettings struct, which influences
	// what is added into the QueryStats.
	LogSettings(context.Context, *LogSettings)

	// LogQuery logs a query when for the given QueryStats.
	LogQuery(context.Context, QueryStats)
}

type logger struct {
	logger *log.Logger
	config LoggerConfig
}

// LoggerConfig is the config used for logger.
type LoggerConfig struct {
	// Dispatch logging asynchronously (logs may arrive out of order which can be confusing, but it won't block function calls).
	LogAsynchronously bool

	// Show time taken by the query.
	ShowTimeTaken bool

	// Show caller (filename and line number).
	ShowCaller bool

	// Show fetched results.
	ShowResults int

	// If true, logs are shown as plaintext (no color).
	NoColor bool

	// Verbose query interpolation, which shows the query before and after
	// interpolating query arguments. The logged query is interpolated by
	// default, InterpolateVerbose only controls whether the query before
	// interpolation is shown. To disable query interpolation entirely, look at
	// HideArgs.
	InterpolateVerbose bool

	// Explicitly hides arguments when logging the query (only the query
	// placeholders will be shown).
	HideArgs bool
}

var _ Logger = (*logger)(nil)

var defaultLogger = NewLogger(os.Stdout, "", log.LstdFlags, LoggerConfig{
	ShowTimeTaken: true,
	ShowCaller:    true,
})

var verboseLogger = NewLogger(os.Stdout, "", log.LstdFlags, LoggerConfig{
	ShowTimeTaken:      true,
	ShowCaller:         true,
	ShowResults:        5,
	InterpolateVerbose: true,
})

// NewLogger returns a new Logger.
func NewLogger(w io.Writer, prefix string, flag int, config LoggerConfig) Logger {
	return &logger{
		logger: log.New(w, prefix, flag),
		config: config,
	}
}

// LogSettings implements the Logger interface.
func (l *logger) LogSettings(ctx context.Context, settings *LogSettings) {
	settings.LogAsynchronously = l.config.LogAsynchronously
	settings.IncludeTime = l.config.ShowTimeTaken
	settings.IncludeCaller = l.config.ShowCaller
	settings.IncludeResults = l.config.ShowResults
}

// LogQuery implements the Logger interface.
func (l *logger) LogQuery(ctx context.Context, queryStats QueryStats) {
	var reset, red, green, blue, purple string
	envNoColor, _ := strconv.ParseBool(os.Getenv("NO_COLOR"))
	if !l.config.NoColor && !envNoColor {
		reset = colorReset
		red = colorRed
		green = colorGreen
		blue = colorBlue
		purple = colorPurple
	}
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if queryStats.Err == nil {
		buf.WriteString(green + "[OK]" + reset)
	} else {
		buf.WriteString(red + "[FAIL]" + reset)
	}
	if l.config.HideArgs {
		buf.WriteString(" " + queryStats.Query + ";")
	} else if !l.config.InterpolateVerbose {
		if queryStats.Err != nil {
			buf.WriteString(" " + queryStats.Query + ";")
			if len(queryStats.Args) > 0 {
				buf.WriteString(" [")
			}
			for i := 0; i < len(queryStats.Args); i++ {
				if i > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(fmt.Sprintf("%#v", queryStats.Args[i]))
			}
			if len(queryStats.Args) > 0 {
				buf.WriteString("]")
			}
		} else {
			query, err := Sprintf(queryStats.Dialect, queryStats.Query, queryStats.Args)
			if err != nil {
				query += " " + err.Error()
			}
			buf.WriteString(" " + query + ";")
		}
	}
	if queryStats.Err != nil {
		errStr := queryStats.Err.Error()
		if i := strings.IndexByte(errStr, '\n'); i < 0 {
			buf.WriteString(blue + " err" + reset + "={" + queryStats.Err.Error() + "}")
		}
	}
	if l.config.ShowTimeTaken {
		buf.WriteString(blue + " timeTaken" + reset + "=" + queryStats.TimeTaken.String())
	}
	if queryStats.RowCount.Valid {
		buf.WriteString(blue + " rowCount" + reset + "=" + strconv.FormatInt(queryStats.RowCount.Int64, 10))
	}
	if queryStats.RowsAffected.Valid {
		buf.WriteString(blue + " rowsAffected" + reset + "=" + strconv.FormatInt(queryStats.RowsAffected.Int64, 10))
	}
	if queryStats.LastInsertId.Valid {
		buf.WriteString(blue + " lastInsertId" + reset + "=" + strconv.FormatInt(queryStats.LastInsertId.Int64, 10))
	}
	if queryStats.Exists.Valid {
		buf.WriteString(blue + " exists" + reset + "=" + strconv.FormatBool(queryStats.Exists.Bool))
	}
	if l.config.ShowCaller {
		buf.WriteString(blue + " caller" + reset + "=" + queryStats.CallerFile + ":" + strconv.Itoa(queryStats.CallerLine) + ":" + filepath.Base(queryStats.CallerFunction))
	}
	if !l.config.HideArgs && l.config.InterpolateVerbose {
		buf.WriteString("\n" + purple + "----[ Executing query ]----" + reset)
		buf.WriteString("\n" + queryStats.Query + "; " + fmt.Sprintf("%#v", queryStats.Args))
		buf.WriteString("\n" + purple + "----[ with bind values ]----" + reset)
		query, err := Sprintf(queryStats.Dialect, queryStats.Query, queryStats.Args)
		query += ";"
		if err != nil {
			query += " " + err.Error()
		}
		buf.WriteString("\n" + query)
	}
	if l.config.ShowResults > 0 && queryStats.Err == nil {
		buf.WriteString("\n" + purple + "----[ Fetched result ]----" + reset)
		buf.WriteString(queryStats.Results)
		if queryStats.RowCount.Int64 > int64(l.config.ShowResults) {
			buf.WriteString("\n...\n(Fetched " + strconv.FormatInt(queryStats.RowCount.Int64, 10) + " rows)")
		}
	}
	if buf.Len() > 0 {
		l.logger.Println(buf.String())
	}
}

// Log wraps a DB and adds logging to it.
func Log(db DB) interface {
	DB
	Logger
} {
	return struct {
		DB
		Logger
	}{DB: db, Logger: defaultLogger}
}

// VerboseLog wraps a DB and adds verbose logging to it.
func VerboseLog(db DB) interface {
	DB
	Logger
} {
	return struct {
		DB
		Logger
	}{DB: db, Logger: verboseLogger}
}

var defaultLogSettings atomic.Value

// SetDefaultLogSettings sets the function to configure the default
// LogSettings. This value is not used unless SetDefaultLogQuery is also
// configured.
func SetDefaultLogSettings(logSettings func(context.Context, *LogSettings)) {
	defaultLogSettings.Store(logSettings)
}

var defaultLogQuery atomic.Value

// SetDefaultLogQuery sets the default logging function to call for all
// queries (if a logger is not explicitly passed in).
func SetDefaultLogQuery(logQuery func(context.Context, QueryStats)) {
	defaultLogQuery.Store(logQuery)
}

type loggerStruct struct {
	logSettings func(context.Context, *LogSettings)
	logQuery    func(context.Context, QueryStats)
}

var _ Logger = (*loggerStruct)(nil)

func (l *loggerStruct) LogSettings(ctx context.Context, logSettings *LogSettings) {
	if l.logSettings == nil {
		return
	}
	l.logSettings(ctx, logSettings)
}

func (l *loggerStruct) LogQuery(ctx context.Context, queryStats QueryStats) {
	if l.logQuery == nil {
		return
	}
	l.logQuery(ctx, queryStats)
}

type slogger struct {
	ctx context.Context
	sl  *slog.Logger
	lv  slog.Leveler
	cfg LoggerConfig
}

var _ Logger = (*slogger)(nil)

func NewSlogger(sl *slog.Logger, lv slog.Leveler, cfg LoggerConfig) Logger {
	return &slogger{
		ctx: context.Background(),
		sl:  sl,
		lv:  lv,
		cfg: cfg,
	}
}

func (l *slogger) LogSettings(ctx context.Context, settings *LogSettings) {
	settings.LogAsynchronously = l.cfg.LogAsynchronously
	settings.IncludeTime = l.cfg.ShowTimeTaken
	settings.IncludeCaller = l.cfg.ShowCaller
	settings.IncludeResults = l.cfg.ShowResults
}

func (l *slogger) LogQuery(ctx context.Context, stats QueryStats) {
	attrs := []slog.Attr{
		slog.String("dialect", stats.Dialect),
		slog.String("query", stats.Query),
		slog.Time("started_at", stats.StartedAt),
		slog.String("results", stats.Results),
	}
	if l.cfg.ShowTimeTaken {
		attrs = append(attrs, slog.Duration("time_taken", stats.TimeTaken))
	}
	if l.cfg.ShowCaller {
		attrs = append(attrs,
			slog.String("caller_file", stats.CallerFile),
			slog.Int("caller_line", stats.CallerLine),
			slog.String("caller_function", stats.CallerFunction),
		)
	}
	l.sl.LogAttrs(l.ctx, l.lv.Level(), "Query stats", attrs...)
}
