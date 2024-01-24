/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/MonetDB/MonetDB-Go/src/mapi"
)

type Conn struct {
	mapi *mapi.MapiConn
	timezone *time.Location
}

func newConn(name string) (*Conn, error) {
	conn := &Conn{
		mapi: nil,
	}

	// For now we do not change the timezone, because this might certain users.
	// conn.timezone = time.Local
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
	conn.setServerTimezone()
	return conn, nil
}

func (c *Conn) setServerTimezone() error {
	tz, err := time.LoadLocation(c.timezone.String())
	if err != nil {
		return err
	}
	if tz.String() != c.timezone.String() {
		return err
	}
	_, offset := time.Now().Zone()

	hours := int(offset / 3600)
	remaining := offset - 3600 * hours
	minutes := int(remaining / 60)
	// Go does not have an absolute value function for int
	if minutes < 0 {
		minutes = -1 * minutes
	}
	query := fmt.Sprintf("SET TIME ZONE INTERVAL '%+03d:%02d' HOUR TO MINUTE;", hours, minutes)
	err = executeStmt(c, query)
	return err
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query, true), nil
}

func (c *Conn) Close() error {
	// TODO: close prepared statements
	c.mapi.Disconnect()
	c.mapi = nil
	return nil
}

func (c *Conn) begin(readonly bool, isolation driver.IsolationLevel) (driver.Tx, error) {
	t := newTx(c)
	var query string
	if readonly {
		// The monetdb documentation mentions this options, but it is not supported
		query = "START TRANSACTION READ ONLY"
	} else {
		switch isolation {
		case driver.IsolationLevel(sql.LevelDefault):
			query = "START TRANSACTION"
		case driver.IsolationLevel(sql.LevelReadUncommitted):
			query = "START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
		case driver.IsolationLevel(sql.LevelReadCommitted):
			query = "START TRANSACTION ISOLATION LEVEL READ COMMITTED"
		case driver.IsolationLevel(sql.LevelRepeatableRead):
			query = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ"
		case driver.IsolationLevel(sql.LevelSerializable):
			query = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE"
		default:
			err := fmt.Errorf("monetdb: unsupported transaction level")
			t.err = err
			return t, t.err
		}
	}

	err := executeStmt(c, query)

	if err != nil {
		t.err = err
	}

	return t, t.err
}

// Deprecated: Use BeginTx instead
func (c *Conn) Begin() (driver.Tx, error) {
	return c.begin(false, driver.IsolationLevel(sql.LevelDefault))
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.begin(opts.ReadOnly, opts.Isolation)
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
