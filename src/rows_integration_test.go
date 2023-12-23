/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql"
	"testing"
)
 
func TestRowsIntegration(t *testing.T) {
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
		result, err := db.Exec("create table test1 ( name varchar(16))")
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
		if rId != 0 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 0 {
			t.Errorf("Unexpected number of rows %d", nRows)
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
		defer rows.Close()
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
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
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		result, err := db.Exec("drop table test1")
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
		if rId != 0 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 0 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
	})

	defer db.Close()
}

func TestColumnTypeIntegration(t *testing.T) {
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
		_, err := db.Exec("create table test1 ( name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Exec insert row", func(t *testing.T) {
		_, err := db.Exec("insert into test1 values ( 'name1' )")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Get Columns", func(t *testing.T) {
		rows, err := db.Query("select * from test1")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		columnlist, err  := rows.Columns()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columnlist {
			if column != "name" {
				t.Errorf("unexpected column name in Columns")
			}
		}
		columntypes, err  := rows.ColumnTypes()
		if err != nil {
			t.Error(err)
		}
		for _, column := range columntypes {
			if column.Name() != "name" {
				t.Errorf("unexpected column name in ColumnTypes")
			}
			length, length_ok := column.Length()
			if length_ok {
				if length != 16 {
					t.Errorf("unexpected column length in ColumnTypes")
				}
			} else {
				t.Error("column length not available")
			}
			_, nullable_ok := column.Nullable()
			if nullable_ok {
				t.Errorf("not expected that decimal size was provided")
			}
			coltype := column.DatabaseTypeName()
			if coltype != "VARCHAR" {
				t.Errorf("unexpected column type")
			}
			scantype := column.ScanType()
			if scantype.Name() != "string" {
				t.Errorf("unexpected scan type")
			}
			_, _, ok := column.DecimalSize()
			if ok {
				t.Errorf("not expected that decimal size was provided")
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
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test1")
		if err != nil {
			t.Fatal(err)
		}
	})

	defer db.Close()
}
