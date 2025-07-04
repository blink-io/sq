package sq

import (
	"context"
	"testing"

	"github.com/blink-io/sq/internal/testutil"
)

func TestSQLiteUpdateQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("a")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := SQLite.Update(a).Returning(a.FIRST_NAME).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectSQLite)
		fields := q1.GetFetchableFields()
		if diff := testutil.Diff(fields, []Field{a.FIRST_NAME}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Fatal(testutil.Callers(), "field should not have been set")
		}
		q1.ReturningFields = q1.ReturningFields[:0]
		_, ok = q1.SetFetchableFields([]Field{a.LAST_NAME})
		if !ok {
			t.Fatal(testutil.Callers(), "field should have been set")
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			Where(a.ACTOR_ID.EqInt(1), a.LAST_UPDATE.IsNotNull()).
			Returning(a.ACTOR_ID)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor AS a" +
			" SET first_name = $1, last_name = $2" +
			" WHERE a.actor_id = $3 AND a.last_update IS NOT NULL" +
			" RETURNING a.actor_id"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("SetFunc", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			SetFunc(func(ctx context.Context, col *Column) {
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
			}).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor AS a" +
			" SET first_name = $1, last_name = $2" +
			" WHERE a.actor_id = $3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("UPDATE with JOIN", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLite.
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			From(a).
			Join(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			LeftJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "UPDATE actor AS a" +
			" SET first_name = $1, last_name = $2" +
			" FROM actor AS a" +
			" JOIN actor AS a ON a.actor_id = a.actor_id" +
			" LEFT JOIN actor AS a ON a.actor_id = a.actor_id" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)" +
			" WHERE a.actor_id = $3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})
}

func TestPostgresUpdateQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("a")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := Postgres.Update(a).Returning(a.FIRST_NAME).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectPostgres)
		fields := q1.GetFetchableFields()
		if diff := testutil.Diff(fields, []Field{a.FIRST_NAME}); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Fatal(testutil.Callers(), "field should not have been set")
		}
		q1.ReturningFields = q1.ReturningFields[:0]
		_, ok = q1.SetFetchableFields([]Field{a.LAST_NAME})
		if !ok {
			t.Fatal(testutil.Callers(), "field should have been set")
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			Where(a.ACTOR_ID.EqInt(1), a.LAST_UPDATE.IsNotNull()).
			Returning(a.ACTOR_ID)
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor AS a" +
			" SET first_name = $1, last_name = $2" +
			" WHERE a.actor_id = $3 AND a.last_update IS NOT NULL" +
			" RETURNING a.actor_id"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("SetFunc", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			SetFunc(func(ctx context.Context, col *Column) {
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
			}).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor AS a" +
			" SET first_name = $1, last_name = $2" +
			" WHERE a.actor_id = $3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("UPDATE with JOIN", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = Postgres.
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			From(a).
			Join(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			LeftJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			FullJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "UPDATE actor AS a" +
			" SET first_name = $1, last_name = $2" +
			" FROM actor AS a" +
			" JOIN actor AS a ON a.actor_id = a.actor_id" +
			" LEFT JOIN actor AS a ON a.actor_id = a.actor_id" +
			" FULL JOIN actor AS a ON a.actor_id = a.actor_id" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)" +
			" WHERE a.actor_id = $3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})
}

func TestMySQLUpdateQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("a")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := MySQL.Update(a).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectMySQL)
		fields := q1.GetFetchableFields()
		if len(fields) != 0 {
			t.Error(testutil.Callers(), "expected 0 fields but got %v", fields)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Fatal(testutil.Callers(), "field should not have been set")
		}
		q1.ReturningFields = q1.ReturningFields[:0]
		_, ok = q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Fatal(testutil.Callers(), "field should not have been set")
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor AS a" +
			" SET a.first_name = ?, a.last_name = ?" +
			" WHERE a.actor_id = ?"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("SetFunc", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			SetFunc(func(ctx context.Context, col *Column) {
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
			}).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor AS a" +
			" SET a.first_name = ?, a.last_name = ?" +
			" WHERE a.actor_id = ?"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("UPDATE with JOIN, ORDER BY, LIMIT", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = MySQL.
			Update(a).
			Join(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			LeftJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			FullJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			CrossJoin(a).
			CustomJoin(",", a).
			JoinUsing(a, a.FIRST_NAME, a.LAST_NAME).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			Where(a.ACTOR_ID.EqInt(1)).
			OrderBy(a.ACTOR_ID).
			Limit(5)
		tt.wantQuery = "UPDATE actor AS a" +
			" JOIN actor AS a ON a.actor_id = a.actor_id" +
			" LEFT JOIN actor AS a ON a.actor_id = a.actor_id" +
			" FULL JOIN actor AS a ON a.actor_id = a.actor_id" +
			" CROSS JOIN actor AS a" +
			" , actor AS a" +
			" JOIN actor AS a USING (first_name, last_name)" +
			" SET a.first_name = ?, a.last_name = ?" +
			" WHERE a.actor_id = ?" +
			" ORDER BY a.actor_id" +
			" LIMIT ?"
		tt.wantArgs = []any{"bob", "the builder", 1, 5}
		tt.assert(t)
	})
}

func TestSQLServerUpdateQuery(t *testing.T) {
	type ACTOR struct {
		TableStruct
		ACTOR_ID    NumberField
		FIRST_NAME  StringField
		LAST_NAME   StringField
		LAST_UPDATE TimeField
	}
	a := New[ACTOR]("")

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := SQLServer.Update(a).SetDialect("lorem ipsum")
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		q1 = q1.SetDialect(DialectSQLServer)
		fields := q1.GetFetchableFields()
		if len(fields) != 0 {
			t.Error(testutil.Callers(), "expected 0 fields but got %v", fields)
		}
		_, ok := q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Fatal(testutil.Callers(), "field should not have been set")
		}
		q1.ReturningFields = q1.ReturningFields[:0]
		_, ok = q1.SetFetchableFields([]Field{a.LAST_NAME})
		if ok {
			t.Fatal(testutil.Callers(), "field should not have been set")
		}
	})

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor" +
			" SET first_name = @p1, last_name = @p2" +
			" WHERE actor.actor_id = @p3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("SetFunc", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			With(NewCTE("cte", nil, Queryf("SELECT 1"))).
			Update(a).
			SetFunc(func(ctx context.Context, col *Column) {
				col.SetString(a.FIRST_NAME, "bob")
				col.SetString(a.LAST_NAME, "the builder")
			}).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "WITH cte AS (SELECT 1)" +
			" UPDATE actor" +
			" SET first_name = @p1, last_name = @p2" +
			" WHERE actor.actor_id = @p3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})

	t.Run("UPDATE with JOIN", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = SQLServer.
			Update(a).
			Set(
				a.FIRST_NAME.SetString("bob"),
				a.LAST_NAME.SetString("the builder"),
			).
			From(a).
			Join(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			LeftJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			FullJoin(a, a.ACTOR_ID.Eq(a.ACTOR_ID)).
			CrossJoin(a).
			CustomJoin(",", a).
			Where(a.ACTOR_ID.EqInt(1))
		tt.wantQuery = "UPDATE actor" +
			" SET first_name = @p1, last_name = @p2" +
			" FROM actor" +
			" JOIN actor ON actor.actor_id = actor.actor_id" +
			" LEFT JOIN actor ON actor.actor_id = actor.actor_id" +
			" FULL JOIN actor ON actor.actor_id = actor.actor_id" +
			" CROSS JOIN actor" +
			" , actor" +
			" WHERE actor.actor_id = @p3"
		tt.wantArgs = []any{"bob", "the builder", 1}
		tt.assert(t)
	})
}

func TestUpdateQuery(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		q1 := UpdateQuery{UpdateTable: Expr("tbl"), Dialect: "lorem ipsum"}
		if diff := testutil.Diff(q1.GetDialect(), "lorem ipsum"); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	})

	f1, f2, f3 := Expr("f1"), Expr("f2"), Expr("f3")
	columMapper := func(ctx context.Context, col *Column) {
		col.set(f1, 1)
		col.set(f2, 2)
		col.set(f3, 3)
	}

	t.Run("PolicyTable", func(t *testing.T) {
		t.Parallel()
		var tt TestTable
		tt.item = UpdateQuery{
			UpdateTable:    policyTableStub{policy: And(Expr("1 = 1"), Expr("2 = 2"))},
			ColumnMapper:   columMapper,
			WherePredicate: Expr("3 = 3"),
		}
		tt.wantQuery = "UPDATE policy_table_stub SET f1 = ?, f2 = ?, f3 = ? WHERE (1 = 1 AND 2 = 2) AND 3 = 3"
		tt.wantArgs = []any{1, 2, 3}
		tt.assert(t)
	})

	notOKTests := []TestTable{{
		description: "nil UpdateTable not allowed",
		item: UpdateQuery{
			UpdateTable:  nil,
			ColumnMapper: columMapper,
		},
	}, {
		description: "empty Assignments not allowed",
		item: UpdateQuery{
			UpdateTable: Expr("tbl"),
			Assignments: nil,
		},
	}, {
		description: "mysql does not support FROM",
		item: UpdateQuery{
			Dialect:      DialectMySQL,
			UpdateTable:  Expr("tbl"),
			FromTable:    Expr("tbl"),
			ColumnMapper: columMapper,
		},
	}, {
		description: "dialect does not allow JOIN without FROM",
		item: UpdateQuery{
			Dialect:     DialectPostgres,
			UpdateTable: Expr("tbl"),
			FromTable:   nil,
			JoinTables: []JoinTable{
				Join(Expr("tbl"), Expr("1 = 1")),
			},
			ColumnMapper: columMapper,
		},
	}, {
		description: "dialect does not support ORDER BY",
		item: UpdateQuery{
			Dialect:       DialectPostgres,
			UpdateTable:   Expr("tbl"),
			ColumnMapper:  columMapper,
			OrderByFields: Fields{f1},
		},
	}, {
		description: "dialect does not support LIMIT",
		item: UpdateQuery{
			Dialect:      DialectPostgres,
			UpdateTable:  Expr("tbl"),
			ColumnMapper: columMapper,
			LimitRows:    5,
		},
	}, {
		description: "dialect does not support RETURNING",
		item: UpdateQuery{
			Dialect:         DialectMySQL,
			UpdateTable:     Expr("tbl"),
			ColumnMapper:    columMapper,
			ReturningFields: Fields{f1, f2, f3},
		},
	}}

	for _, tt := range notOKTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertNotOK(t)
		})
	}

	errTests := []TestTable{{
		description: "ColumnMapper err",
		item: UpdateQuery{
			UpdateTable:  Expr("tbl"),
			ColumnMapper: func(context.Context, *Column) { panic(ErrFaultySQL) },
		},
	}, {
		description: "UpdateTable Policy err",
		item: UpdateQuery{
			UpdateTable:  policyTableStub{err: ErrFaultySQL},
			ColumnMapper: columMapper,
		},
	}, {
		description: "FromTable Policy err",
		item: UpdateQuery{
			UpdateTable:  Expr("tbl"),
			FromTable:    policyTableStub{err: ErrFaultySQL},
			ColumnMapper: columMapper,
		},
	}, {
		description: "JoinTables Policy err",
		item: UpdateQuery{
			UpdateTable:  Expr("tbl"),
			ColumnMapper: columMapper,
			FromTable:    Expr("tbl"),
			JoinTables: []JoinTable{
				Join(policyTableStub{err: ErrFaultySQL}, Expr("1 = 1")),
			},
		},
	}, {
		description: "CTEs err",
		item: UpdateQuery{
			CTEs:         []CTE{NewCTE("cte", nil, Queryf("SELECT {}", FaultySQL{}))},
			UpdateTable:  Expr("tbl"),
			ColumnMapper: columMapper,
		},
	}, {
		description: "UpdateTable err",
		item: UpdateQuery{
			UpdateTable:  FaultySQL{},
			ColumnMapper: columMapper,
		},
	}, {
		description: "not mysql Assignments err",
		item: UpdateQuery{
			Dialect:     DialectPostgres,
			UpdateTable: Expr("tbl"),
			Assignments: []Assignment{FaultySQL{}},
		},
	}, {
		description: "FromTable err",
		item: UpdateQuery{
			Dialect:      DialectPostgres,
			UpdateTable:  Expr("tbl"),
			ColumnMapper: columMapper,
			FromTable:    FaultySQL{},
		},
	}, {
		description: "JoinTables err",
		item: UpdateQuery{
			Dialect:      DialectPostgres,
			UpdateTable:  Expr("tbl"),
			ColumnMapper: columMapper,
			FromTable:    Expr("tbl"),
			JoinTables: []JoinTable{
				Join(FaultySQL{}, Expr("1 = 1")),
			},
		},
	}, {
		description: "mysql Assignments err",
		item: UpdateQuery{
			Dialect:     DialectMySQL,
			UpdateTable: Expr("tbl"),
			Assignments: []Assignment{FaultySQL{}},
		},
	}, {
		description: "WherePredicate Variadic err",
		item: UpdateQuery{
			UpdateTable:    Expr("tbl"),
			ColumnMapper:   columMapper,
			WherePredicate: And(FaultySQL{}),
		},
	}, {
		description: "WherePredicate err",
		item: UpdateQuery{
			UpdateTable:    Expr("tbl"),
			ColumnMapper:   columMapper,
			WherePredicate: FaultySQL{},
		},
	}, {
		description: "OrderByFields err",
		item: UpdateQuery{
			Dialect:       DialectMySQL,
			UpdateTable:   Expr("tbl"),
			ColumnMapper:  columMapper,
			OrderByFields: Fields{FaultySQL{}},
		},
	}, {
		description: "LimitRows err",
		item: UpdateQuery{
			Dialect:       DialectMySQL,
			UpdateTable:   Expr("tbl"),
			ColumnMapper:  columMapper,
			OrderByFields: Fields{f1},
			LimitRows:     FaultySQL{},
		},
	}, {
		description: "ReturningFields err",
		item: UpdateQuery{
			Dialect:         DialectPostgres,
			UpdateTable:     Expr("tbl"),
			ColumnMapper:    columMapper,
			ReturningFields: Fields{FaultySQL{}},
		},
	}}

	for _, tt := range errTests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			tt.assertErr(t, ErrFaultySQL)
		})
	}
}
