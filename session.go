package gorm

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"time"
)

type Session struct {
	Config       *Config
	Error        error
	RowsAffected int64
	Statement    *Statement
	DB           *DB
	PrepareStmt  bool
	preparedStmtDB *PreparedStmtDB
	cacheStore   sync.Map
	context      context.Context
	ctx          context.Context
	tx           *sql.Tx
	rollback     bool
	committed    bool
}

func (session *Session) Begin() *Session {
	if session.tx != nil {
		return session
	}

	tx, err := session.DB.BeginTx(session.ctx, nil)
	if err != nil {
		session.Error = err
		return session
	}
	session.tx = tx
	session.rollback = false
	session.committed = false

	if session.PrepareStmt && session.preparedStmtDB != nil {
		session.ctx = session.preparedStmtDB.SetTxInContext(session.ctx, tx)
	}

	return session
}

func (session *Session) Rollback() *Session {
	if session.tx == nil {
		session.Error = errors.New("no transaction to rollback")
		return session
	}

	if session.PrepareStmt && session.preparedStmtDB != nil {
		session.preparedStmtDB.InvalidateTxCache(session.tx)
	}

	err := session.tx.Rollback()
	if err != nil {
		session.Error = err
		return session
	}
	session.rollback = true
	session.tx = nil
	return session
}

func (session *Session) Commit() *Session {
	if session.tx == nil {
		session.Error = errors.New("no transaction to commit")
		return session
	}

	if session.PrepareStmt && session.preparedStmtDB != nil {
		session.preparedStmtDB.InvalidateTxCache(session.tx)
	}

	err := session.tx.Commit()
	if err != nil {
		session.Error = err
		return session
	}
	session.committed = true
	session.tx = nil
	return session
}

func (session *Session) Exec(query string, values ...interface{}) *Session {
	if session.Error != nil {
		return session
	}

	if session.tx != nil {
		if session.PrepareStmt && session.preparedStmtDB != nil {
			if stmt, ok := session.preparedStmtDB.GetTxCachedStmt(session.tx, query); ok {
				_, err := stmt.ExecContext(session.ctx, values...)
				if err != nil {
					session.Error = err
				}
				return session
			}
		}
		_, err := session.tx.ExecContext(session.ctx, query, values...)
		if err != nil {
			session.Error = err
		}
		return session
	}

	if session.PrepareStmt && session.preparedStmtDB != nil {
		if stmt, ok := session.preparedStmtDB.GetCachedStmt(query); ok {
			_, err := stmt.ExecContext(session.ctx, values...)
			if err != nil {
				session.Error = err
			}
			return session
		}
	}

	_, err := session.DB.ExecContext(session.ctx, query, values...)
	if err != nil {
		session.Error = err
	}
	return session
}

func (session *Session) Query(query string, values ...interface{}) (*sql.Rows, error) {
	if session.Error != nil {
		return nil, session.Error
	}

	if session.tx != nil {
		if session.PrepareStmt && session.preparedStmtDB != nil {
			if stmt, ok := session.preparedStmtDB.GetTxCachedStmt(session.tx, query); ok {
				return stmt.QueryContext(session.ctx, values...)
			}
		}
		return session.tx.QueryContext(session.ctx, query, values...)
	}

	if session.PrepareStmt && session.preparedStmtDB != nil {
		if stmt, ok := session.preparedStmtDB.GetCachedStmt(query); ok {
			return stmt.QueryContext(session.ctx, values...)
		}
	}

	return session.DB.QueryContext(session.ctx, query, values...)
}

func (session *Session) QueryRow(query string, values ...interface{}) *sql.Row {
	if session.Error != nil {
		return &sql.Row{}
	}

	if session.tx != nil {
		if session.PrepareStmt && session.preparedStmtDB != nil {
			if stmt, ok := session.preparedStmtDB.GetTxCachedStmt(session.tx, query); ok {
				return stmt.QueryRowContext(session.ctx, values...)
			}
		}
		return session.tx.QueryRowContext(session.ctx, query, values...)
	}

	if session.PrepareStmt && session.preparedStmtDB != nil {
		if stmt, ok := session.preparedStmtDB.GetCachedStmt(query); ok {
			return stmt.QueryRowContext(session.ctx, values...)
		}
	}

	return session.DB.QueryRowContext(session.ctx, query, values...)
}

func (session *Session) Close() {
	if session.preparedStmtDB != nil {
		session.preparedStmtDB.Close()
	}
}
