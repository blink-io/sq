package sq

import (
	"context"
	"database/sql"
	"github.com/bokwoon95/sq"
)

type (
	Txer interface {
		Begin() (*sql.Tx, error)
		BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	}

	TxDB interface {
		DB
		Txer
	}

	RunInTxer interface {
		RunInTx(context.Context, *sql.TxOptions, func(context.Context, sq.DB) error) error
	}

	txDB struct {
		TxDB
	}
)

func (db txDB) RunInTx(ctx context.Context, opts *sql.TxOptions, fn func(context.Context, sq.DB) error) error {
	return RunInTx(ctx, db, opts, fn)
}

func InTx(txdb TxDB) interface {
	TxDB
	RunInTxer
} {
	return txDB{TxDB: txdb}
}

func RunInTx(ctx context.Context, db TxDB, opts *sql.TxOptions, fn func(context.Context, sq.DB) error) error {
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	var done bool

	defer func() {
		if !done {
			_ = tx.Rollback()
		}
	}()

	if err := fn(ctx, tx); err != nil {
		return err
	}

	done = true
	return tx.Commit()
}
