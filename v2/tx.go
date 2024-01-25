/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

type Tx struct {
	conn *Conn
	err  error
}

func newTx(c *Conn) *Tx {
	return &Tx{
		conn: c,
		err:  nil,
	}
}

func (t *Tx) Commit() error {
	err := executeStmt(t.conn, "COMMIT")
	if err != nil {
		t.err = err
	}

	return err
}

func (t *Tx) Rollback() error {
	err := executeStmt(t.conn, "ROLLBACK")
	if err != nil {
		t.err = err
	}

	return err
}
