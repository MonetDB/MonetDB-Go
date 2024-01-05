/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"context"
	"database/sql/driver"

	"github.com/MonetDB/MonetDB-Go/src/mapi"
)

type Conn struct {
	mapi *mapi.MapiConn
}

func newConn(name string) (*Conn, error) {
	conn := &Conn{
		mapi: nil,
	}

	m, err := mapi.NewMapi(name)
	if err != nil {
		return conn, err
	}
	errConn := m.Connect()
	if errConn != nil {
		return conn, errConn
	}

	conn.mapi = m
	m.SetSizeHeader(true)
	return conn, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query, true), nil
}

func (c *Conn) Close() error {
	// TODO: close prepared statements
	// TODO: close contexts
	c.mapi.Disconnect()
	c.mapi = nil
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	t := newTx(c)
	err := executeStmt(c, "START TRANSACTION")

	if err != nil {
		t.err = err
	}

	return t, t.err
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Begin()
	return tx, err
}

func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	stmt := newStmt(c, query, false)
	res, err := stmt.ExecContext(ctx, args)
	defer stmt.Close()

	return res, err
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// QueryContext may return ErrSkip.
	// QueryContext must honor the context timeout and return when the context is canceled.
	stmt := newStmt(c, query, false)
	res, err := stmt.QueryContext(ctx, args)
	defer stmt.Close()

	return res, err
}

func (c *Conn) PrepareContext(ctx context.Context, query string) (Stmt, error) {
	stmt := newStmt(c, query, true)
	return *stmt, nil
}

func (c *Conn) CheckNamedValue(arg *driver.NamedValue) error {
	_, err := mapi.ConvertToMonet(arg.Value)
	return err
}
