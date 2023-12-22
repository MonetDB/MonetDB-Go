/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"

	"github.com/MonetDB/MonetDB-Go/src/mapi"
)

type Conn struct {
	mapi   *mapi.MapiConn
}

func newConn(name string) (*Conn, error) {
	conn := &Conn{
		mapi:   nil,
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
	return newStmt(c, query), nil
}

func (c *Conn) Close() error {
	c.mapi.Disconnect()
	c.mapi = nil
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	t := newTx(c)

	_, err := c.execute("START TRANSACTION")
	if err != nil {
		t.err = err
	}

	return t, t.err
}

func (c *Conn) execute(query string) (string, error) {
	if c.mapi == nil {
		return "", fmt.Errorf("monetdb: database connection is closed")
	}
	return c.mapi.Execute(query)
}
