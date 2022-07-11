package mysql

import (
	"context"
	"database/sql"
)

// query
type query struct {
	MysqlDB *MysqlDB
}

//QueryRowContext executes a query that is expected to return at most one row.
//Use it when transaction is necessary
func (q *query) QueryRowContext(c context.Context, query string, args ...interface{}) *sql.Row {
	ctx, tx := GetTrans(c)
	if tx != nil {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return q.MysqlDB.RDB().QueryRowContext(ctx, query, args...)
}

// ExecContext executes a query that doesn't return rows.
// For example: an INSERT and UPDATE.
// Use it when transaction is necessary
func (q *query) ExecContext(c context.Context, query string, args ...interface{}) (sql.Result, error) {
	ctx, tx := GetTrans(c)
	if tx != nil {
		return tx.ExecContext(ctx, query, args...)
	}
	return q.MysqlDB.WDB().ExecContext(ctx, query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// Use it when transaction is necessary
func (q *query) QueryContext(c context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	ctx, tx := GetTrans(c)
	if tx != nil {
		return tx.QueryContext(ctx, query, args)
	}
	return q.MysqlDB.RDB().QueryContext(ctx, query, args...)
}

// PrepareContext creates a prepared statement for  later queries or executions..
// Use it when transaction is necessary
func (q *query) PrepareContext(c context.Context, query string) (*sql.Stmt, error) {
	ctx, tx := GetTrans(c)
	if tx != nil {
		return tx.PrepareContext(ctx, query)
	}
	return q.MysqlDB.WDB().PrepareContext(ctx, query)
}

// Stmt returns a transaction-specific prepared statement from an existing statement.
//Use it when transaction is necessary
func (q *query) Stmt(c context.Context, stmt *sql.Stmt) *sql.Stmt {
	_, tx := GetTrans(c)
	return tx.Stmt(stmt)
}
