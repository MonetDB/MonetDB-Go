/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"

	"github.com/MonetDB/MonetDB-Go/src/mapi"
)

type Stmt struct {
	conn  *Conn
	query string
	resultset mapi.ResultSet
}

func newStmt(c *Conn, q string) *Stmt {
	s := &Stmt{
		conn:   c,
		query:  q,
	}
	s.resultset.Metadata.ExecId = -1
	return s
}

func (s *Stmt) Close() error {
	s.conn = nil
	return nil
}

func (s *Stmt) NumInput() int {
	return -1
}

func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
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

func (s *Stmt) copyRows(rowwlist [][]mapi.Value)([][]driver.Value) {
	res := make([][]driver.Value, s.resultset.Metadata.RowCount)
	for i, row := range rowwlist {
		res[i] = make([]driver.Value, s.resultset.Metadata.ColumnCount)
		for j, col := range row {
			res[i][j] = col
		}
	}
	return res
}

func (s *Stmt) copyArgs(arglist []driver.Value)([]mapi.Value) {
	res := make([]mapi.Value, len(arglist))
	for i, arg := range arglist {
			res[i] = arg
	}
	return res
}

func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	rows := newRows(s)

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
	rows.rows = s.copyRows(s.resultset.Rows)
	rows.schema = s.resultset.Schema

	return rows, rows.err
}

func (s *Stmt) exec(args []driver.Value) (string, error) {
	if s.resultset.Metadata.ExecId == -1 {
		err := s.prepareQuery()
		if err != nil {
			return "", err
		}
	}

	arglist := s.copyArgs(args)
	execStr, err := s.resultset.CreateExecString(arglist)
	if err != nil {
		return "", err
	} 
	return s.conn.execute(execStr)
}

func (s *Stmt) prepareQuery() error {
	q := fmt.Sprintf("PREPARE %s", s.query)
	r, err := s.conn.execute(q)
	if err != nil {
		return err
	}

	return s.resultset.StoreResult(r)
}
