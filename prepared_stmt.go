package gorm

import (
	"context"
	"database/sql"
	"sync"
)

type PreparedStmtDB struct {
	Stmts       map[string]*Stmt
	PreparedSQL []string
	Mux         sync.RWMutex
	ConnPool
}

type Stmt struct {
	PrepareStmt *sql.Stmt
	Tx          *sql.Tx
	TxStmt      *sql.Stmt
	IsTx        bool
}

func (db *PreparedStmtDB) GetDBConn() (*sql.DB, error) {
	if dbConnector, ok := db.ConnPool.(GetDBConnector); ok && dbConnector != nil {
		return dbConnector.GetDBConn()
	}

	sqlDB, err := db.ConnPool.(*sql.DB)
	if err != nil {
		return nil, err
	}

	return sqlDB, nil
}

func (db *PreparedStmtDB) Close() {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	for _, stmt := range db.Stmts {
		if stmt.PrepareStmt != nil {
			stmt.PrepareStmt.Close()
		}
	}
}

func (db *PreparedStmtDB) Prepare(ctx context.Context, query string) (*Stmt, error) {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	if stmt, ok := db.Stmts[query]; ok {
		return stmt, nil
	}

	sqlDB, err := db.GetDBConn()
	if err != nil {
		return nil, err
	}

	stmt, err := sqlDB.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	db.Stmts[query] = &Stmt{PrepareStmt: stmt}
	db.PreparedSQL = append(db.PreparedSQL, query)

	return db.Stmts[query], nil
}

func (db *PreparedStmtDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}

	if stmt.IsTx {
		return stmt.TxStmt.QueryContext(ctx, args...)
	}

	return stmt.PrepareStmt.QueryContext(ctx, args...)
}

func (db *PreparedStmtDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}

	if stmt.IsTx {
		return stmt.TxStmt.ExecContext(ctx, args...)
	}

	return stmt.PrepareStmt.ExecContext(ctx, args...)
}

func (db *PreparedStmtDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	stmt, err := db.Prepare(ctx, query)
	if err != nil {
		return &sql.Row{}
	}

	if stmt.IsTx {
		return stmt.TxStmt.QueryRowContext(ctx, args...)
	}

	return stmt.PrepareStmt.QueryRowContext(ctx, args...)
}

func (db *PreparedStmtDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	sqlDB, err := db.GetDBConn()
	if err != nil {
		return nil, err
	}

	tx, err := sqlDB.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return tx, nil
}

func (db *PreparedStmtDB) Begin(ctx context.Context) (*sql.Tx, error) {
	return db.BeginTx(ctx, nil)
}

func (db *PreparedStmtDB) Rollback(tx *sql.Tx) error {
	if tx == nil {
		return nil
	}

	err := tx.Rollback()
	if err != nil {
		return err
	}

	// Invalidate all cached prepared statements that were created within this transaction
	db.Mux.Lock()
	defer db.Mux.Unlock()

	for query, stmt := range db.Stmts {
		if stmt.IsTx && stmt.Tx == tx {
			// Close the transaction-scoped statement
			if stmt.TxStmt != nil {
				stmt.TxStmt.Close()
			}
			// Remove from cache
			delete(db.Stmts, query)
			// Also remove from PreparedSQL slice
			for i, q := range db.PreparedSQL {
				if q == query {
					db.PreparedSQL = append(db.PreparedSQL[:i], db.PreparedSQL[i+1:]...)
					break
				}
			}
		}
	}

	return nil
}

func (db *PreparedStmtDB) Commit(tx *sql.Tx) error {
	if tx == nil {
		return nil
	}

	err := tx.Commit()
	if err != nil {
		return err
	}

	// After commit, transaction-scoped statements are no longer valid
	db.Mux.Lock()
	defer db.Mux.Unlock()

	for query, stmt := range db.Stmts {
		if stmt.IsTx && stmt.Tx == tx {
			// Close the transaction-scoped statement
			if stmt.TxStmt != nil {
				stmt.TxStmt.Close()
			}
			// Remove from cache
			delete(db.Stmts, query)
			// Also remove from PreparedSQL slice
			for i, q := range db.PreparedSQL {
				if q == query {
					db.PreparedSQL = append(db.PreparedSQL[:i], db.PreparedSQL[i+1:]...)
					break
				}
			}
		}
	}

	return nil
}

func (db *PreparedStmtDB) SetTxStmt(tx *sql.Tx, stmt *Stmt) {
	db.Mux.Lock()
	defer db.Mux.Unlock()

	stmt.IsTx = true
	stmt.Tx = tx
	stmt.TxStmt = tx.Stmt(stmt.PrepareStmt)
}
