package gorm

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"
	"time"
)

type PreparedStmtDB struct {
	StmtCache     map[string]*StmtCacheEntry
	StmtCacheLock sync.RWMutex
	TxStmtCache   map[*sql.Tx]map[string]*StmtCacheEntry
	TxStmtLock    sync.Mutex
	ConnPool
}

type StmtCacheEntry struct {
	Stmt         *sql.Stmt
	LastUsedTime time.Time
	Query        string
	IsTxBound    bool
}

func (pdb *PreparedStmtDB) Prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	pdb.StmtCacheLock.RLock()
	if stmt, ok := pdb.StmtCache[query]; ok {
		pdb.StmtCacheLock.RUnlock()
		stmt.LastUsedTime = time.Now()
		return stmt.Stmt, nil
	}
	pdb.StmtCacheLock.RUnlock()

	pdb.StmtCacheLock.Lock()
	defer pdb.StmtCacheLock.Unlock()

	if stmt, ok := pdb.StmtCache[query]; ok {
		stmt.LastUsedTime = time.Now()
		return stmt.Stmt, nil
	}

	tx, ok := ctx.Value(txCtxKey{}).(*sql.Tx)
	if ok && tx != nil {
		// For transaction-scoped prepared statements, do not cache globally
		stmt, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		entry := &StmtCacheEntry{
			Stmt:      stmt,
			Query:     query,
			IsTxBound: true,
		}
		pdb.TxStmtLock.Lock()
		if pdb.TxStmtCache[tx] == nil {
			pdb.TxStmtCache[tx] = make(map[string]*StmtCacheEntry)
		}
		pdb.TxStmtCache[tx][query] = entry
		pdb.TxStmtLock.Unlock()
		return stmt, nil
	}

	stmt, err := pdb.ConnPool.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	pdb.StmtCache[query] = &StmtCacheEntry{
		Stmt:         stmt,
		LastUsedTime: time.Now(),
		Query:        query,
		IsTxBound:    false,
	}
	return stmt, nil
}

func (pdb *PreparedStmtDB) GetCachedStmt(query string) (*sql.Stmt, bool) {
	pdb.StmtCacheLock.RLock()
	defer pdb.StmtCacheLock.RUnlock()
	stmt, ok := pdb.StmtCache[query]
	if ok {
		stmt.LastUsedTime = time.Now()
		return stmt.Stmt, true
	}
	return nil, false
}

func (pdb *PreparedStmtDB) GetTxCachedStmt(tx *sql.Tx, query string) (*sql.Stmt, bool) {
	pdb.TxStmtLock.Lock()
	defer pdb.TxStmtLock.Unlock()
	if txCache, ok := pdb.TxStmtCache[tx]; ok {
		if entry, ok := txCache[query]; ok {
			return entry.Stmt, true
		}
	}
	return nil, false
}

func (pdb *PreparedStmtDB) InvalidateTxCache(tx *sql.Tx) {
	pdb.TxStmtLock.Lock()
	defer pdb.TxStmtLock.Unlock()
	if txCache, ok := pdb.TxStmtCache[tx]; ok {
		for _, entry := range txCache {
			if entry.Stmt != nil {
				entry.Stmt.Close()
			}
		}
		delete(pdb.TxStmtCache, tx)
	}
}

func (pdb *PreparedStmtDB) Close() {
	pdb.StmtCacheLock.Lock()
	defer pdb.StmtCacheLock.Unlock()
	for _, entry := range pdb.StmtCache {
		if entry.Stmt != nil {
			entry.Stmt.Close()
		}
	}
	pdb.StmtCache = make(map[string]*StmtCacheEntry)

	pdb.TxStmtLock.Lock()
	defer pdb.TxStmtLock.Unlock()
	for _, txCache := range pdb.TxStmtCache {
		for _, entry := range txCache {
			if entry.Stmt != nil {
				entry.Stmt.Close()
			}
		}
	}
	pdb.TxStmtCache = make(map[*sql.Tx]map[string]*StmtCacheEntry)
}

// Implement ConnPool interface methods
func (pdb *PreparedStmtDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if stmt, ok := pdb.GetCachedStmt(query); ok {
		return stmt.ExecContext(ctx, args...)
	}
	return pdb.ConnPool.ExecContext(ctx, query, args...)
}

func (pdb *PreparedStmtDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if stmt, ok := pdb.GetCachedStmt(query); ok {
		return stmt.QueryContext(ctx, args...)
	}
	return pdb.ConnPool.QueryContext(ctx, query, args...)
}

func (pdb *PreparedStmtDB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	if stmt, ok := pdb.GetCachedStmt(query); ok {
		return stmt.QueryRowContext(ctx, args...)
	}
	return pdb.ConnPool.QueryRowContext(ctx, query, args...)
}

func (pdb *PreparedStmtDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return pdb.ConnPool.BeginTx(ctx, opts)
}

func (pdb *PreparedStmtDB) Rollback(tx *sql.Tx) error {
	pdb.InvalidateTxCache(tx)
	return tx.Rollback()
}

func (pdb *PreparedStmtDB) Commit(tx *sql.Tx) error {
	pdb.InvalidateTxCache(tx)
	return tx.Commit()
}

// Helper to extract tx from context
type txCtxKey struct{}

func (pdb *PreparedStmtDB) SetTxInContext(ctx context.Context, tx *sql.Tx) context.Context {
	return context.WithValue(ctx, txCtxKey{}, tx)
}

func (pdb *PreparedStmtDB) GetTxFromContext(ctx context.Context) *sql.Tx {
	tx, ok := ctx.Value(txCtxKey{}).(*sql.Tx)
	if ok {
		return tx
	}
	return nil
}
