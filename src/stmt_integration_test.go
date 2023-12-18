/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

 package monetdb

 import (
	 "database/sql"
	 "testing"
 )
 
func TestStmtIntegration(t *testing.T) {
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
		_, err := db.Exec("create table test3 ( id int, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Prepare statement", func(t *testing.T) {

		stmt, err := db.Prepare("insert into test3 values ( ?, ? )")
		if err != nil {
			t.Fatal(err)
		}
		if stmt == nil {
			t.Fatal("Statement is nil")
		}
		result, err := stmt.Exec(1, "name1" )
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
		defer stmt.Close()
	})

	t.Run("Run simple query", func(t *testing.T) {
		stmt, err := db.Prepare("select * from test3 where id = ?")
		if err != nil {
			t.Fatal(err)
		}
		if stmt == nil {
			t.Fatal("Statement is nil")
		}
		rows, err := stmt.Query(1)
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("query returned no rows")
		}
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
		defer stmt.Close()
	})

	t.Run("Get Columns", func(t *testing.T) {
		stmt, err := db.Prepare("select * from test3 where id = ?")
		if err != nil {
			t.Logf(err.Error())
		}
		if stmt == nil {
			t.Fatal("Statement is nil")
		}
		rows, err := stmt.Query(1)
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("query returned empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Logf(err.Error())
		}
		for i, column := range columnlist {
			if i == 0 && column != "id" {
				t.Errorf("Name of column %d not correct", i)
			}
			if i == 1 && column != "name" {
				t.Errorf("Name of column %d not correct", i)
			}
		} 
		defer rows.Close()
		defer stmt.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test3")
		if err != nil {
			t.Error(err)
		}
	})

	defer db.Close()
}