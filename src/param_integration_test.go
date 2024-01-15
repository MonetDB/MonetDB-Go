/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
*/
package monetdb

import (
	"database/sql"
	"testing"
)
 
func TestParamIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test1 ( name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.Exec("insert into test1 values ( 'name1' )")
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
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get Columns", func(t *testing.T) {
		// Be careful, a named placeholder starts with a colon but does not end with one
		rows, err := db.Query("select * from test1 where name = :name", sql.Named("name", "name1"))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Error("unexpected column name")
			}
		}
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test1")
		if err != nil {
			t.Error(err)
		}
	})
}

func TestIntParamIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test1 ( name integer)")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.Exec("insert into test1 values ( 16 )")
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
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name int
			if err := rows.Scan(&name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select * from test1 where name = :name", sql.Named("name", 16))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Error("unexpected column name")
			}
		}
		for rows.Next() {
			var name int
			if err := rows.Scan(&name); err != nil {
			 t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test1")
		if err != nil {
			t.Error(err)
		}
	})
}

func TestMultipleParamIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test1 ( name varchar(16), value integer)")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		result, err := db.Exec("insert into test1 values ( 'name1', 16 )")
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
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var name string
			var val int
			if err := rows.Scan(&name, &val); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select * from test1 where name = :name and value = :value", sql.Named("name", "name1"), sql.Named("value", 16))
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()

		if rows == nil {
			t.Fatal("empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for i, column := range columnlist {
			if i == 0 {
				if column != "name" {
					t.Error("unexpected column name")
				}
			}
			if i == 1 {
				if column != "value" {
					t.Error("unexpected column name")
				}
			}
		}
		for rows.Next() {
			var name string
			var value int
			if err := rows.Scan(&name, &value); err != nil {
				t.Error(err)
			}
			if name != "name1" {
				t.Error("unexpected value for name field")
			}
			if value != 16 {
				t.Error("unexpected value for value field")
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test1")
		if err != nil {
			t.Error(err)
		}
	})
}
