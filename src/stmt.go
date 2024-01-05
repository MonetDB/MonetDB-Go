/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"context"
	"database/sql/driver"

	"github.com/MonetDB/MonetDB-Go/src/mapi"
)

type Stmt struct {
	isPreparedStatement bool
	conn  *Conn
	query mapi.Query
	resultset mapi.ResultSet
}

func newStmt(c *Conn, q string, prepare bool) *Stmt {
	s := &Stmt{
		conn:   c,
		isPreparedStatement: prepare,
	}
	s.resultset.Metadata.ExecId = -1
	s.query.Mapi = c.mapi
	s.query.SqlQuery = q
	return s
}

func executeStmt(c *Conn, query string) error {
	stmt := newStmt(c, query, false)
	_, err := stmt.Exec(nil)
	defer stmt.Close()
	return err
}

func (s *Stmt) Close() error {
	// TODO: check if this is correct, the pool should handle the connections
	s.conn = nil
	return nil
}

func (s *Stmt) NumInput() int {
	return -1
}

// Deprecated: Use ExecContext instead
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	arglist := ValueArgs(args)
	return s.execResult(arglist)
}

func (s *Stmt) execResult(args []driver.NamedValue) (driver.Result, error) {
	res := newResult()

	r, err := s.exec(args)
	if err != nil {
		res.err = err
		return res, res.err
	}

	err = s.resultset.StoreResult(r)
	res.lastInsertId = s.resultset.Metadata.LastRowId
	res.rowsAffected = s.resultset.Metadata.RowCount
	res.err = err

	return res, res.err
}

func copyRows(rowlist [][]mapi.Value, rowcount int, columncount int)([][]driver.Value) {
	res := make([][]driver.Value, rowcount)
	for i, row := range rowlist {
		res[i] = make([]driver.Value, columncount)
		for j, col := range row {
			res[i][j] = col
		}
	}
	return res
}

func copyArgs(arglist []driver.Value)([]mapi.Value) {
	res := make([]mapi.Value, len(arglist))
	for i, arg := range arglist {
		res[i] = arg
	}
	return res
}

func ArgNames(arglist []driver.NamedValue)([]string) {
	res := make([]string, len(arglist))
	for i, arg := range arglist {
		res[i] = arg.Name
	}
	return res
}

func ArgValues(arglist []driver.NamedValue)([]driver.Value) {
	res := make([]driver.Value, len(arglist))
	for i, arg := range arglist {
			res[i] = arg.Value
	}
	return res
}

// Deprecated: This function is only needed for backward compatibility
// of the Exec and Query methods. This functions will be removed
// when the other deprecated functions are removed.

// Previously the argument list was an arry of values. The new api uses an
// array of NamedValues. This function creates the new array by copying the
// Value field and setting the Ordinal field. Now all the functions that
// are not deprecated can use the new argument list type.
func ValueArgs(arglist []driver.Value)([]driver.NamedValue) {
	res := make([]driver.NamedValue, len(arglist))
	for i, arg := range arglist {
		res[i].Ordinal = i - 1
		res[i].Value = arg
	}
	return res
}

// Deprecated: Use QueryContext instead
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	arglist := ValueArgs(args)
	return s.queryResult(arglist)
}

func (s *Stmt) queryResult(args []driver.NamedValue) (driver.Rows, error) {
	rows := newRows(s.conn.mapi, &s.resultset)
	r, err := s.exec(args)
	if err != nil {
		rows.err = err
		return rows, rows.err
	}

	err = s.resultset.StoreResult(r)
	if err != nil {
		rows.err = err
		return rows, rows.err
	}
	rows.queryId = s.resultset.Metadata.QueryId
	rows.lastRowId = s.resultset.Metadata.LastRowId
	rows.rowCount = s.resultset.Metadata.RowCount
	rows.offset = s.resultset.Metadata.Offset
	rows.rows = copyRows(s.resultset.Rows, s.resultset.Metadata.RowCount, s.resultset.Metadata.ColumnCount)
	rows.schema = s.resultset.Schema

	return rows, rows.err
}


func (s *Stmt) exec(args []driver.NamedValue) (string, error) {
	if s.isPreparedStatement && s.resultset.Metadata.ExecId == -1 {
		err := s.query.PrepareQuery(&s.resultset)
		if err != nil {
			return "", err
		}
	}

	if len(args) != 0 {
		if s.isPreparedStatement {
			arglist := copyArgs(ArgValues(args))
			return s.query.ExecutePreparedQuery(&s.resultset, arglist)
		} else {
			argnames := ArgNames(args)
			arglist := copyArgs(ArgValues(args))
			return s.query.ExecuteNamedQuery(&s.resultset, argnames, arglist)
		}
	} else {
		return s.query.ExecuteQuery(&s.resultset)
	}
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	res, err := s.execResult(args)
	return res, err
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.queryResult(args)
	return res, err
}

func (s *Stmt) CheckNamedValue(arg *driver.NamedValue) error {
	_, err := mapi.ConvertToMonet(arg.Value)
	return err
}