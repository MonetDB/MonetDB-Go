/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"context"
	"database/sql/driver"

	"github.com/MonetDB/MonetDB-Go/v2/mapi"
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
// Run the command on the database with a new context, without a timeout
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	queryParams := paramFromValuesList(args)
	return s.execResult(context.Background(), queryParams)
}

// This function executes a mapi command inside a goroutine. This makes it possible to cancel
// the command when the context is cancelled. At this point in time MonetDB does not support cancelling
// a running query. This feature is planned for the next release. When that comes available, we will add
// a function call that cancels the query when a timeout occurs before it is finished.
func (s *Stmt) mapiDo(ctx context.Context, args []driver.NamedValue) (string, error) {
	type res struct {
		resultstring string;
		err error
	}
	c := make(chan res, 1)

    go func() {
		r, err := s.exec(args)
		result := res{r, err}
		c <- result
		}()

    select {
    case <-ctx.Done():
        <-c // Wait for the goroutine to return. Later we need to cancel the query on the database
        return "", ctx.Err()
    case result := <-c:
        return result.resultstring, result.err
    }
}

func (s *Stmt) execResult(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	res := newResult()
	r, err := s.mapiDo(ctx, args)
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

func convertRows(rows [][]mapi.Value, columncount int)([][]driver.Value) {
	res := make([][]driver.Value, len(rows))
	for i, row := range rows {
		res[i] = make([]driver.Value, columncount)
		for j, col := range row {
			res[i][j] = col
		}
	}
	return res
}

func convertParamValues(args []driver.Value)([]mapi.Value) {
	res := make([]mapi.Value, len(args))
	for i, arg := range args {
		res[i] = arg
	}
	return res
}

func paramNamesList(args []driver.NamedValue)([]string) {
	res := make([]string, len(args))
	for i, arg := range args {
		res[i] = arg.Name
	}
	return res
}

func paramValuesList(args []driver.NamedValue)([]driver.Value) {
	res := make([]driver.Value, len(args))
	for i, arg := range args {
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
func paramFromValuesList(args []driver.Value)([]driver.NamedValue) {
	res := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		res[i].Ordinal = i - 1
		res[i].Value = arg
	}
	return res
}

// Deprecated: Use QueryContext instead
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	queryParams := paramFromValuesList(args)
	return s.queryResult(context.Background(), queryParams)
}

func (s *Stmt) queryResult(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows := newRows(s.conn.mapi, &s.resultset)
	r, err := s.mapiDo(ctx, args)
	if err != nil {
		rows.err = err
		return rows, rows.err
	}

	err = s.resultset.StoreResult(r)
	if err != nil {
		rows.err = err
		return rows, rows.err
	}
	// We have gotten the first batch of the resultset. The RowCount is the total number of rows in the result.
	// But we have only at most mapi.MAPI_ARRAY_SIZE rows available.
	rows.queryId = s.resultset.Metadata.QueryId
	rows.lastRowId = s.resultset.Metadata.LastRowId
	rows.rowCount = s.resultset.Metadata.RowCount
	rows.offset = s.resultset.Metadata.Offset
	rows.rows = convertRows(s.resultset.Rows, s.resultset.Metadata.ColumnCount)
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
			queryParams := convertParamValues(paramValuesList(args))
			return s.query.ExecutePreparedQuery(&s.resultset, queryParams)
		} else {
			queryParamsNames := paramNamesList(args)
			queryParams := convertParamValues(paramValuesList(args))
			return s.query.ExecuteNamedQuery(&s.resultset, queryParamsNames, queryParams)
		}
	} else {
		return s.query.ExecuteQuery(&s.resultset)
	}
}

func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	res, err := s.execResult(ctx, args)
	return res, err
}

func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	res, err := s.queryResult(ctx, args)
	return res, err
}

func (s *Stmt) CheckNamedValue(arg *driver.NamedValue) error {
	_, err := mapi.ConvertToMonet(arg.Value)
	return err
}