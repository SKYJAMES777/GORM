package gorm

import (
	"context"
	"database/sql"
	"sync"
)

type PreparedStmtDB struct {
	Tx        *sql.Tx
	TxCtx     context.Context
	TxStmts   map[string]*sql.Stmt
	Stmts     map[string]*sql.Stmt
	Mux       sync.RWMutex
	PreparedSQL []string
	ConnPool  ConnPool
}

func (db *PreparedStmtDB) GetDBConn() (*sql.DB, error) {
	if db.Tx != nil {
		return nil, nil
	}
	if pool, ok := db.ConnPool.(*DB); ok {
		return pool.DB(), nil
	}
	return nil, nil
}

func (db *PreparedStmtDB) Close() {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	for _, stmt := range db.Stmts {
		stmt.Close()
	}
	for _, stmt := range db.TxStmts {
		stmt.Close()
	}
	db.Stmts = nil
	db.TxStmts = nil
	db.PreparedSQL = nil
}

func (db *PreparedStmtDB) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	if db.Tx != nil {
		if stmt, ok := db.TxStmts[query]; ok {
			return stmt, nil
		}
		stmt, err := db.Tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		db.TxStmts[query] = stmt
		return stmt, nil
	}

	if stmt, ok := db.Stmts[query]; ok {
		return stmt, nil
	}

	conn, err := db.ConnPool.(*DB).ConnPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	db.Stmts[query] = stmt
	db.PreparedSQL = append(db.PreparedSQL, query)
	return stmt, nil
}

func (db *PreparedStmtDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (db *PreparedStmtDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (db *PreparedStmtDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return &sql.Row{}
	}
	return stmt.QueryRowContext(ctx, args...)
}

// RollbackAndCleanupTx cleans up transaction-scoped prepared statements after rollback.
func (db *PreparedStmtDB) RollbackAndCleanupTx() error {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	if db.Tx == nil {
		return nil
	}

	// Close all transaction-scoped prepared statements
	for _, stmt := range db.TxStmts {
		stmt.Close()
	}
	db.TxStmts = make(map[string]*sql.Stmt)

	// Rollback the transaction
	err := db.Tx.Rollback()
	db.Tx = nil
	db.TxCtx = nil
	return err
}

// CommitAndCleanupTx cleans up transaction-scoped prepared statements after commit.
func (db *PreparedStmtDB) CommitAndCleanupTx() error {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	if db.Tx == nil {
		return nil
	}

	// Close all transaction-scoped prepared statements
	for _, stmt := range db.TxStmts {
		stmt.Close()
	}
	db.TxStmts = make(map[string]*sql.Stmt)

	// Commit the transaction
	err := db.Tx.Commit()
	db.Tx = nil
	db.TxCtx = nil
	return err
}
