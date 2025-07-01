package sq

import (
	"context"
)

type ExtTable[M any, S any] interface {
	Table

	ColumnMapper(ss ...S) ColumnMapper

	RowMapper(context.Context, *Row) M
}

type Executor[T ExtTable[M, S], M any, S any] interface {
	Insert(ctx context.Context, db DB, ss ...S) (Result, error)

	Update(ctx context.Context, db DB, where Predicate, s S) (Result, error)

	Delete(ctx context.Context, db DB, where Predicate) (Result, error)

	One(ctx context.Context, db DB, where Predicate) (M, error)

	All(ctx context.Context, db DB, where Predicate) ([]M, error)

	Table() T
}

type executor[T ExtTable[M, S], M any, S any] struct {
	t T
}

func NewExecutor[T ExtTable[M, S], M any, S any](t T) Executor[T, M, S] {
	return executor[T, M, S]{t: t}
}

func (e executor[T, M, S]) Insert(ctx context.Context, db DB, ss ...S) (Result, error) {
	q := InsertInto(e.t).
		ColumnValues(e.t.ColumnMapper(ss...))
	return ExecContext(ctx, db, q)
}

func (e executor[T, M, S]) Table() T {
	return e.t
}

func (e executor[T, M, S]) Update(ctx context.Context, db DB, where Predicate, s S) (Result, error) {
	q := Update(e.t).
		SetFunc(e.t.ColumnMapper(s)).
		Where(where)
	return ExecContext(ctx, db, q)
}

func (e executor[T, M, S]) Delete(ctx context.Context, db DB, where Predicate) (Result, error) {
	q := DeleteFrom(e.t).
		Where(where)
	return ExecContext(ctx, db, q)
}

func (e executor[T, M, S]) One(ctx context.Context, db DB, where Predicate) (M, error) {
	q := From(e.t).
		Where(where).
		Limit(1)
	return FetchOneContext[M](ctx, db, q, e.t.RowMapper)
}

func (e executor[T, M, S]) All(ctx context.Context, db DB, where Predicate) ([]M, error) {
	q := From(e.t).
		Where(where)
	return FetchAllContext[M](ctx, db, q, e.t.RowMapper)
}
