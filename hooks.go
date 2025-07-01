package sq

import (
	"context"
	"database/sql"
	"time"
)

type (
	HookEvent struct {
		DB *DB

		Query     string
		Args      []any
		StartTime time.Time
		Result    sql.Result
		Err       error

		Stash map[any]any
	}

	Hook interface {
		BeforeQuery(context.Context, *HookEvent) context.Context
		AfterQuery(context.Context, *HookEvent)
	}

	hookDB struct {
		DB
		hooks []Hook
	}
)

func Hooks(db DB, hooks ...Hook) interface {
	DB
} {
	return hookDB{DB: db, hooks: hooks}
}

func (db hookDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	ctx, event := db.beforeQuery(ctx, query, args...)
	rows, err := db.DB.QueryContext(ctx, query, args...)
	db.afterQuery(ctx, event, nil, err)
	return rows, err
}

func (db hookDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	ctx, event := db.beforeQuery(ctx, query, args...)
	res, err := db.DB.ExecContext(ctx, query, args...)
	db.afterQuery(ctx, event, res, err)
	return res, err
}

func (db hookDB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.DB.PrepareContext(ctx, query)
}

func (db hookDB) beforeQuery(ctx context.Context, query string, args ...any) (context.Context, *HookEvent) {
	if len(db.hooks) == 0 {
		return ctx, nil
	}

	event := &HookEvent{
		Query: query,
		Args:  args,

		StartTime: time.Now(),
	}

	for _, hook := range db.hooks {
		ctx = hook.BeforeQuery(ctx, event)
	}

	return ctx, event
}

func (db hookDB) afterQuery(
	ctx context.Context,
	event *HookEvent,
	res sql.Result,
	err error,
) {
	if event == nil {
		return
	}

	event.Result = res
	event.Err = err

	db.afterQueryFromIndex(ctx, event, len(db.hooks)-1)
}

var _ Hook = logHook{}

func (db hookDB) afterQueryFromIndex(ctx context.Context, event *HookEvent, hookIndex int) {
	for ; hookIndex >= 0; hookIndex-- {
		db.hooks[hookIndex].AfterQuery(ctx, event)
	}
}

type logHook struct {
	logf func(ctx context.Context, format string, args ...any)
}

func (l logHook) BeforeQuery(ctx context.Context, event *HookEvent) context.Context {
	l.logf(ctx, event.Query, event.Args...)
	return ctx
}

func (l logHook) AfterQuery(ctx context.Context, event *HookEvent) {
	if event.Err != nil {
		l.logf(ctx, "[logHook] Query failed: %s\n", event.Err)
	} else {
		l.logf(ctx, "[logHook] Query completed in %s\n", event.StartTime)
	}
}
