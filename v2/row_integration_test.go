/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"testing"
)
 
func TestRowIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test2 ( id int, name varchar(16))")
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.Exec("insert into test2 values ( 1, 'name1' )"); 
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != -1 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 1 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	t.Run("Run simple query", func(t *testing.T) {
		var name string
		err := db.QueryRow("select name from test2 where id = 1").Scan(&name)
		switch {
		case err == sql.ErrNoRows:
			t.Error("Query did not return a result")
		case err != nil:
			t.Error(err)
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test2")
		if err != nil {
			t.Error(err)
		}
	})

	defer db.Close()
}